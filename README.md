AlephClient
===========

API client for Aleph API.


Installation
============
Install using `pip`.

```
pip install git+https://github.com/alephdata/alephclient
```

Usage
=====

`alephclient` needs the url of an Aleph API instance and an API key for the said
API. These can be provided by setting the environment variables `ALEPH_HOST` and
`ALEPH_API_KEY` respectively; or by passing them in with `--api-base-url` and
`--api-key` options.

Commands
--------
- `crawldir`

The crawldir command crawls through a given directory recursively and uploads
all the files and directories inside it to a collection. The foreign id of the
collection needs to be passed to the command with `--foreign-id` option. If a
new collection has to be created for the given foreign id, then we also need to
pass a category name with `--category` option. Optionally, language and country
hint can be passed with `--language` and `--country` options. The `path` argument
needs to be a valid path to a directory

example:
```
alephclient --api-base-url http://127.0.0.1:5000/api/2/ --api-key 2c0ae66024f0403bb751207e54c5eb5d crawldir --foreign-id wikileaks-cable --category leak /Users/sunu/data/cable
```

License
=======
MIT