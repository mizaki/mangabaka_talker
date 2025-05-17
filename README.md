# MangaUpdates plugin for Comic Tagger

A plugin for [Comic Tagger](https://github.com/comictagger/comictagger/releases) to allow the use of the metadata from [MangaBaka](https://mangabaka.dev).

## Installation

The easiest installation method as of ComicTagger 1.6.0-alpha.23 for the plugin is to place the [release](https://github.com/mizaki/mangabaka_talker/releases) zip file
`mangabaka_talker-plugin-<version>.zip` (or wheel `.whl`) into the [plugins](https://github.com/comictagger/comictagger/wiki/Installing-plugins) directory.

## Development Installation

You can build the wheel with `tox run -m build` or clone ComicTagger and clone the talker and install the talker into the ComicTagger environment `pip install -e .`
