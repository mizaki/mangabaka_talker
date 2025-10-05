"""
MangaBaka information source
"""
# Copyright comictagger team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import argparse
import json
import logging
import pathlib
import tarfile
import time
from typing import Any, Callable, TypedDict, cast
from urllib.parse import urlencode, urljoin

import settngs
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, ImageHash, MetadataOrigin
from comictalker import talker_utils
from comictalker.comiccacher import ComicCacher
from comictalker.comiccacher import Series as CCSeries
from comictalker.comictalker import ComicTalker, RLCallBack, TalkerDataError, TalkerError, TalkerNetworkError
from comictalker.vendor.pyrate_limiter import Duration, Limiter, RequestRate

try:
    import niquests as requests
except ImportError:
    import requests
logger = logging.getLogger(__name__)


MBTYPES = ["", "manga", "novel", "manhwa", "manhua", "oel", "other"]
MBSTATUS = ["cancelled", "completed", "hiatus", "releasing", "unknown", "upcoming"]
MBSTATE = ["active", "merged", "deleted"]
MBRATING = ["safe", "suggestive", "erotica", "pornographic"]


class MBImageURL(TypedDict):
    raw: str
    default: str
    small: str


class MBAnime(TypedDict):
    start: str
    end: str


class MBRelationship(TypedDict, total=False):
    main_story: list[int]
    adaptation: list[int]
    prequel: list[int]
    sequel: list[int]
    side_story: list[int]
    spin_off: list[int]
    alternative: list[int]
    other: list[int]


class MUAuthor(TypedDict):
    name: str
    author_id: int
    type: str


class MBPublisher(TypedDict):
    name: str
    type: str
    note: str


class MBSource(TypedDict):
    id: int | str
    rating: float


class MBSeries(TypedDict, total=False):
    id: int
    state: str
    merged_with: int | None
    title: str
    native_title: str
    romanized_title: str
    secondary_titles: dict[str, list[dict[str, str]]]
    cover: MBImageURL
    authors: list[str]
    artists: list[str]
    description: str
    year: int
    status: str
    is_licensed: bool
    has_anime: bool
    anime: MBAnime | None
    content_rating: str  # safe, suggestive, erotica, pornographic
    type: str
    rating: int
    final_volume: str | None
    final_chapter: str | None
    total_chapters: str | None
    links: list[str] | None
    publishers: list[MBPublisher] | None
    genres: list[str] | None
    tags: list[str] | None
    last_updated_at: str
    relationships: MBRelationship | None
    source: dict[str, MBSource]


class MBPagination(TypedDict):
    count: int
    page: int
    limit: int
    next: str | None
    previous: str | None


class MBResult(TypedDict, total=False):
    status: int
    message: str
    pagination: MBPagination
    data: list[MBSeries] | MBSeries


# https://mangabaka.dev/api
limiter = Limiter(RequestRate(60, Duration.MINUTE))


class MangaBakaTalker(ComicTalker):
    name: str = "MangaBaka"
    id: str = "mangabaka"
    comictagger_min_ver: str = "1.6.0b7"
    logo_url: str = "https://mangabaka.dev/images/logo.png"
    website: str = "https://mangabaka.dev"
    attribution: str = f"Metadata provided by <a href='{website}'>{name}</a>"
    about: str = (
        f"<a href='{website}'>{name}</a> collates and cleanses the data from multiple sources: AniList, Kitsu, "
        f"MangaDex, MangaUpdates, MyAnimeList and Anime News Network."
    )

    def __init__(self, version: str, cache_folder: pathlib.Path):
        super().__init__(version, cache_folder)
        # Settings
        self.default_api_url = self.api_url = "https://api.mangabaka.dev/v1/"
        self.use_series_start_as_volume: bool = False
        self.use_original_publisher: bool = False
        self.filter_dojin: bool = False
        self.filter_type: str = ""
        self.age_filter: str = "safe"
        self.age_filter_range: list[str] = []

        self.total_requests_made: int = 0

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            f"--{self.id}-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
        )
        parser.add_setting(
            f"--{self.id}-use-original-publisher",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use the original publisher",
            help="Use the original publisher instead of English language publisher",
        )
        parser.add_setting(
            f"--{self.id}-age-filter",
            default="safe",
            choices=MBRATING,
            display_name="Age rating filter:",
            help="Select the level of age rating filtering. *Not guaranteed, relies on correct tagging*",
        )
        parser.add_setting(
            f"--{self.id}-filter-dojin",
            default=True,
            action=argparse.BooleanOptionalAction,
            display_name="Filter out dojin results",
            help="Filter out dojin from the search results (Genre: Doujinshi)",
        )
        parser.add_setting(
            f"--{self.id}-filter-type",
            default="",
            choices=MBTYPES,
            display_name="Filter for only type",
            help="Filter out all other 'types' other than selected",
        )
        parser.add_setting(
            f"--{self.id}-download",
            display_name="Download SQLite MangaBaka DB",
            help="Download the SQLite MangaBaka DB to the cache directory",
        )
        parser.add_setting(
            f"--{self.id}-url",
            display_name="API URL",
            help=f"Use the given MangaBaka URL. (default: {self.default_api_url})",
        )
        parser.add_setting(f"--{self.id}-key", file=False, cmdline=False)

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.use_series_start_as_volume = settings["mb_use_series_start_as_volume"]
        self.use_original_publisher = settings["mb_use_original_publisher"]
        self.age_filter = settings["mb_age_filter"]
        self.filter_type = settings["mb_filter_type"]
        self.filter_dojin = settings["mb_filter_dojin"]

        # Create a filter with all accepted age rating
        self.age_filter_range = MBRATING[: MBRATING.index(self.age_filter) + 1]

        return settings

    def check_status(self, settings: dict[str, Any]) -> tuple[str, bool]:
        url = talker_utils.fix_url(settings[f"{self.id}_url"])
        if not url:
            url = self.default_api_url

        try:
            test_url = urljoin(url, "series/10023")
            mb_response = requests.get(test_url, headers={"user-agent": "comictagger/" + self.version}).json()

            if mb_response["status"] == 200:
                return "The URL is valid", True
            else:
                return "The URL is INVALID!", False
        except Exception:
            return "Failed to connect to the URL!", False

    def download_file(self, settings: dict[str, Any], cache_path: pathlib.Path) -> tuple[str, bool]:
        url = talker_utils.fix_url(settings[f"{self.id}_url"])
        if not url:
            url = self.default_api_url

        try:
            test_url = urljoin(url, "database/series.sqlite.tar.gz")
            temp_tar_path = cache_path / "series.sqlite.tar.gz"
            with requests.get(test_url, stream=True, headers={"user-agent": f"comictagger/{self.version}"}) as r:
                if r.status_code != 200:
                    return "Failed to download file!", False
                with open(temp_tar_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            try:
                with tarfile.open(temp_tar_path, "r:gz") as tar:
                    tar.extractall(path=cache_path)
                return "Successfully downloaded MB DB file", True
            except Exception as e:
                logger.error("Failed to extract MB DB file: %s", e)
                return f"Failed to extract MB DB file: {e}", False
        except Exception as e:
            logger.debug("Failed to download MB DB %s", e)
            return f"Failed to connect to the URL! {e}", False

    def search_for_series(
        self,
        series_name: str,
        callback: Callable[[int, int], None] | None = None,
        refresh_cache: bool = False,
        literal: bool = False,
        series_match_thresh: int = 90,
        *,
        on_rate_limit: RLCallBack | None = None,
    ) -> list[ComicSeries]:
        search_series_name = series_name
        logger.info(f"{self.name} searching: {search_series_name}")

        # Before we search online, look in our cache, since we might have done this same search recently
        # For literal searches always retrieve from online
        cvc = ComicCacher(self.cache_folder, self.version)
        if not refresh_cache and not literal:
            cached_search_results = cvc.get_search_results(self.id, series_name)
            if len(cached_search_results) > 0:
                # Unpack to apply any filters
                json_cache: list[MBSeries] = [json.loads(x[0].data) for x in cached_search_results]
                # Always have to filter
                json_cache = self._filter_nsfw(json_cache)
                if self.filter_type:
                    json_cache = self._filter_type(json_cache)
                if self.filter_dojin:
                    json_cache = self._filter_dojin(json_cache)

                return self._format_search_results(json_cache)

        params: dict[str, Any] = {
            "q": search_series_name,
            "content_rating": ["safe", "suggestive", "erotica", "pornographic"],
            "page": 1,
            "limit": 50,
        }

        mb_response: MBResult = self._get_mb_content(
            urljoin(self.api_url, "series/search"), params, on_rate_limit=on_rate_limit
        )
        mb_data: list[MBSeries] = cast(list[MBSeries], mb_response["data"])
        search_results: list[MBSeries] = []

        logger.debug(
            f"Found {mb_response['pagination']['limit'] * mb_response['pagination']['page']} of "
            f"{mb_response['pagination']['count']} results"
        )
        search_results.extend(s for s in mb_data)

        # 1. Don't fetch more than some sane amount of pages.
        # 2. Halt when any result on the current page is less than or equal to a set ratio using thefuzz
        while mb_response["pagination"]["next"] is not None and mb_response["pagination"]["page"] < 6:
            if not literal:
                # Stop searching once any entry falls below the threshold
                stop_searching = any(
                    not utils.titles_match(search_series_name, manga["title"], series_match_thresh) for manga in mb_data
                )

                if stop_searching:
                    break

            mb_response = self._get_mb_content(mb_response["pagination"]["next"], {}, on_rate_limit=on_rate_limit)
            mb_data = cast(list[MBSeries], mb_response["data"])
            search_results.extend(s for s in mb_data)

        # Cache raw data. It's considered "full" for our purposes
        cvc.add_search_results(
            self.id,
            series_name,
            [CCSeries(id=x["id"], data=json.dumps(x).encode("utf-8")) for x in search_results],
            True,
        )

        # Filter any tags AFTER adding to cache
        search_results = self._filter_nsfw(search_results)
        if self.filter_type:
            search_results = self._filter_type(search_results)
        if self.filter_dojin:
            search_results = self._filter_dojin(search_results)

        formatted_search_results = self._format_search_results(search_results)

        return formatted_search_results

    def fetch_comic_data(
        self,
        issue_id: str | None = None,
        series_id: str | None = None,
        issue_number: str = "",
        on_rate_limit: RLCallBack | None = None,
    ) -> GenericMetadata:
        comic_data = GenericMetadata()
        # Could be sent "issue_id" only which is actually series_id
        if issue_id and series_id is None:
            series_id = issue_id

        if series_id is not None:
            return self._map_comic_issue_to_metadata(self._fetch_series(int(series_id), on_rate_limit=on_rate_limit))

        return comic_data

    def fetch_issues_in_series(self, series_id: str, on_rate_limit: RLCallBack | None = None) -> list[GenericMetadata]:
        # MangaBaka has no issue level data (yet)
        return [GenericMetadata()]

    def _get_mb_content(self, url: str, params: dict[str, Any], *, on_rate_limit: RLCallBack | None = None) -> MBResult:
        mb_response: MBResult = self._get_url_content(url, params, on_rate_limit=on_rate_limit)
        if mb_response["status"] != 200:
            logger.debug(f"{self.name} query failed with error #{mb_response['status']}:  [{mb_response['message']}].")
            raise TalkerNetworkError(self.name, 0, f"{mb_response['status']}: {mb_response['message']}")

        return mb_response

    def _get_url_content(self, url: str, params: dict[str, Any], on_rate_limit: RLCallBack | None = None) -> Any:
        # if there is a 500 error, try a few more times before giving up
        limit_counter = 0

        for tries in range(1, 5):
            try:
                with limiter.ratelimit("mb", delay=True, on_rate_limit=on_rate_limit):
                    logger.debug("Requesting: %s?%s", url, urlencode(params))
                    self.total_requests_made += 1
                    resp = requests.get(
                        url, params=params, headers={"user-agent": "comictagger/" + self.version}, timeout=60
                    )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code in (
                    requests.codes.SERVER_ERROR,
                    requests.codes.BAD_GATEWAY,
                    requests.codes.UNAVAILABLE,
                ):
                    logger.debug("Try #%d: %d", tries, resp.status_code)

                elif resp.status_code == requests.codes.TOO_MANY_REQUESTS:
                    logger.info("%s rate limit encountered. Waiting for 10 seconds", self.name)
                    self._log_total_requests()
                    time.sleep(10)
                    limit_counter += 1
                    if limit_counter > 3:
                        # Tried 3 times,
                        logger.error("%s rate limit error. Exceeded 3 retires.", self.name)
                        raise TalkerNetworkError(self.name, 3, "Rate Limit Error")
                else:
                    logger.error("Unknown status code: %d, %s", resp.status_code, resp.content)
                    break

            except requests.exceptions.Timeout:
                logger.debug(f"Connection to {self.name} timed out.")
                if tries > 3:
                    raise TalkerNetworkError(self.name, 4)
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request exception: {e}")
                raise TalkerNetworkError(self.name, 0, str(e)) from e
            except json.JSONDecodeError as e:
                logger.debug(f"JSON decode error: {e}")
                raise TalkerDataError(self.name, 2, "ComicVine did not provide json")
            except TalkerError as e:
                raise e
            except Exception as e:
                raise TalkerNetworkError(self.name, 5, str(e))

        raise TalkerNetworkError(self.name, 5, "Unknown error occurred")

    def _format_search_results(self, search_results: list[MBSeries]) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            formatted_results.append(self._format_series(record))

        return formatted_results

    def _format_secondary_titles(self, titles: dict[str, list[dict[str, str]]]) -> set[str]:
        aliases = set()
        for alias in titles.values():
            if alias is not None:
                for a in alias:
                    aliases.add(a["title"])

        return aliases

    def _format_series(self, series: MBSeries) -> ComicSeries:
        aliases = set()
        if series.get("native_title") is not None:
            aliases.add(series["native_title"])
        if series.get("romanized_title") is not None:
            aliases.add(series["romanized_title"])
        aliases.update(self._format_secondary_titles(series["secondary_titles"]))

        start_year: int | None = None
        if series.get("year"):
            start_year = utils.xlate_int(series["year"])

        publisher = self._filter_publishers(series["publishers"])

        return ComicSeries(
            aliases=aliases,
            count_of_issues=series.get("total_chapters"),
            description=series.get("description", ""),
            id=str(series["id"]),
            image_url=series["cover"].get("default"),
            name=series["title"],
            publisher=publisher,
            start_year=start_year,
            count_of_volumes=series.get("final_volume"),
            format=series["type"],
        )

    def _filter_publishers(self, publishers: list[MBPublisher] | None) -> str | None:
        if publishers is None:
            return None

        publisher_list = []
        for pub in publishers:
            if self.use_original_publisher and pub["type"] == "Original":
                publisher_list.append(pub["name"])
            elif not self.use_original_publisher and pub["type"] == "English":
                publisher_list.append(pub["name"])

        return ", ".join(publisher_list)

    def _filter_nsfw(self, search_results: list[MBSeries]) -> list[MBSeries]:
        filtered_list = []
        for series in search_results:
            if series["content_rating"] in self.age_filter_range:
                filtered_list.append(series)

        return filtered_list

    def _filter_dojin(self, search_results: list[MBSeries]) -> list[MBSeries]:
        filtered_list = []
        for series in search_results:
            if series["genres"] is not None and "doujinshi" not in series["genres"]:
                filtered_list.append(series)

        return filtered_list

    def _filter_type(self, search_results: list[MBSeries]) -> list[MBSeries]:
        filtered_list = []
        for series in search_results:
            if self.filter_type == series["type"]:
                filtered_list.append(series)

        return filtered_list

    def fetch_series(self, series_id: str, on_rate_limit: RLCallBack | None = None) -> ComicSeries:
        return self._format_series(self._fetch_series(int(series_id), on_rate_limit=on_rate_limit))

    def _fetch_series(self, series_id: int, on_rate_limit: RLCallBack | None = None) -> MBSeries:
        # Should almost always have the data cached from search
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series = cvc.get_series_info(str(series_id), self.id)

        if cached_series is not None and cached_series[1]:
            return json.loads(cached_series[0].data)

        series_url = urljoin(self.api_url, f"series/{series_id}")
        mb_response: MBResult = self._get_mb_content(series_url, {}, on_rate_limit=on_rate_limit)
        mb_data: MBSeries = cast(MBSeries, mb_response["data"])

        # Cache raw data
        cvc.add_series_info(
            self.id,
            CCSeries(id=str(series_id), data=json.dumps(mb_data).encode("utf-8")),
            True,
        )

        return mb_data

    def fetch_issues_by_series_issue_num_and_year(
        self,
        series_id_list: list[str],
        issue_number: str,
        year: str | int | None,
        on_rate_limit: RLCallBack | None = None,
    ) -> list[GenericMetadata]:
        series_list = []
        for series_id in series_id_list:
            series_list.append(
                self._map_comic_issue_to_metadata(self._fetch_series(int(series_id), on_rate_limit=on_rate_limit))
            )

        return series_list

    def _map_comic_issue_to_metadata(self, series: MBSeries) -> GenericMetadata:
        md = GenericMetadata(
            data_origin=MetadataOrigin(self.id, self.name),
            series_id=utils.xlate(series["id"]),
            issue_id=utils.xlate(series["id"]),
            series=series["title"],
        )

        md._cover_image = ImageHash(URL=series["cover"]["default"], Hash=0, Kind="")

        if series.get("native_title") is not None:
            md.series_aliases.add(series["native_title"])
        if series.get("romanized_title") is not None:
            md.series_aliases.add(series["romanized_title"])
        md.series_aliases.update(self._format_secondary_titles(series["secondary_titles"]))

        md.publisher = self._filter_publishers(series["publishers"])

        if series["authors"] is not None:
            for author in series["authors"]:
                md.add_credit(author, role="Writer")

        if series["artists"] is not None:
            for artist in series["artists"]:
                md.add_credit(artist, role="Artist")

        if series["type"] == "manga":
            md.manga = "Yes"

        if series["genres"] is not None:
            for genre in series["genres"]:
                md.genres.add(genre)

        if series["tags"] is not None:
            for tag in series["tags"]:
                md.tags.add(tag)

        if series["content_rating"]:
            md.maturity_rating = series["content_rating"].capitalize()

        md.count_of_volumes = utils.xlate_int(series["final_volume"])
        md.count_of_issues = utils.xlate_int(series["final_chapter"])
        md.year = utils.xlate_int(series["year"])
        md.description = series["description"]

        if series["links"] is not None:
            for link in series["links"]:
                try:
                    md.web_links.append(utils.parse_url(link))
                except utils.LocationParseError:
                    ...

        if series["rating"] is not None:
            md.critical_rating = utils.xlate_float(series["rating"] / 2)

        if self.use_series_start_as_volume and md.year:
            md.volume = md.year

        return md
