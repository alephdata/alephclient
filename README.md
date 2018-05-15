alephclient
===========

Command-line client for Aleph 2. It can be used to bulk import document sets via
the API, without direct access to the server. It requires an active API client
to perform uploads.


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
collection needs to be passed to the command with `--foreign-id` option. The
language used in the directory can optionally be specified with the --language
option, which expects a 2-letter ISO 639 language code. It can be specified
multiple times, for when the directory contains files in more than one language.
The `path` argument needs to be a valid path to a directory

example:
```
alephclient --api-base-url http://127.0.0.1:5000/api/2/ --api-key 2c0ae66024f0403bb751207e54c5eb5d crawldir --foreign-id wikileaks-cable --category leak /Users/sunu/data/cable
```

License
=======

MIT License

Copyright (c) 2018 Journalism Development Network, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
