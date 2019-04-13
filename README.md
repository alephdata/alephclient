# alephclient

Command-line client for Aleph. It can be used to bulk import document sets via
the API, without direct access to the server. It requires an active API client
to perform uploads.

## Installation

Install using `pip`.

```bash
pip install alephclient
```

## Usage

`alephclient` needs the url of an Aleph API instance and an API key for the said
API. These can be provided by setting the environment variables `ALEPH_HOST` and
`ALEPH_API_KEY` respectively; or by passing them in with `--api-host` and
`--api-key` options.

### Commands

#### `crawldir`

The crawldir command crawls through a given directory recursively and uploads
all the files and directories inside it to a collection. The foreign id of the
collection needs to be passed to the command with `--foreign-id` option. The
language used in the directory can optionally be specified with the --language
option, which expects a 2-letter ISO 639 language code. It can be specified
multiple times, for when the directory contains files in more than one language.
The `path` argument needs to be a valid path to a directory

Example:

```bash
alephclient --api-base-url http://127.0.0.1:5000/api/2/ --api-key 2c0ae66024f0403bb751207e54c5eb5d crawldir --foreign-id wikileaks-cable --category leak /Users/sunu/data/cable
```

#### `write-entities`

Load JSON-formatted entities formatted in the `followthemoney` structure into
an aleph collection. This can be used in conjunction with the command-line tools
for generating such data provided by `followthemoney-util`. Data that is loaded
this way should be aggregated as much as possible, for example using the
`ftm aggregate` command-line utility, or the `balkhash` database layer.

A typical use might look this:

```bash
ftm map my_mapping.yml | ftm aggregate | alephclient write-entities -f my_dataset
```

#### `stream-entities`

The inverse of `write-entities`, this will stream entities from the given aleph
instance so that they can be written to a file.

Here's how you'd stream an aleph collection:

```bash
alephclient stream-entities -f my_dataset >my_dataset.json
```

#### `bulkload`

The bulkload command executes an entity mapping in the system. Its only argument
is a YAML mapping file.
