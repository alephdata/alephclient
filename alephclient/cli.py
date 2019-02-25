import json
import click
import logging

from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.tasks.crawldir import crawl_dir
from alephclient.tasks.bulkload import bulk_load
from alephclient.util import read_json_stream

log = logging.getLogger(__name__)


@click.group()
@click.option('--api-base-url', help="Aleph API address", envvar="ALEPH_HOST")
@click.option("--api-key", envvar="ALEPH_API_KEY", help="Aleph API key for authentication")  # noqa
@click.option('-r', '--retries', type=int, default=5, help="retries upon server failure")  # noqa
@click.pass_context
def cli(ctx, api_base_url, api_key, retries):
    """API client for Aleph API"""
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    if not api_base_url:
        raise click.BadParameter("Missing Aleph base URL")
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj["api"] = AlephAPI(api_base_url, api_key, retries=retries)


@cli.command()
@click.option('--casefile', is_flag=True, default=False,
              help="handle as case file")
@click.option('--language',
              multiple=True,
              help="language hint: 2-letter language code (ISO 639)")
@click.option('--foreign-id',
              required=True,
              help="foreign_id of the collection")
@click.argument('path', type=click.Path(exists=True))
@click.pass_context
def crawldir(ctx, path, foreign_id, language=None, casefile=False):
    """Crawl a directory recursively and upload the documents in it to a
    collection."""
    try:
        config = {
            'label': path,
            'languages': language,
            'casefile': casefile
        }
        api = ctx.obj["api"]
        crawl_dir(api, path, foreign_id, config)
    except AlephException as exc:
        raise click.ClickException(str(exc))


@cli.command()
@click.argument('mapping_file')
@click.pass_context
def bulkload(ctx, mapping_file):
    """Trigger a load of structured entity data using the submitted mapping."""
    try:
        bulk_load(ctx.obj["api"], mapping_file)
    except AlephException as exc:
        raise click.ClickException(str(exc))


@cli.command('write-entities')
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('-m', '--merge', is_flag=True, default=False, help="update entities in place")  # noqa
@click.pass_context
def write_entities(ctx, foreign_id, merge):
    """Read entities from standard input and index them."""
    stdin = click.get_text_stream('stdin')
    api = ctx.obj["api"]
    try:
        collection = api.load_collection_by_foreign_id(foreign_id, {})
        collection_id = collection.get('id')
        entities = read_json_stream(stdin)
        api.write_entities(collection_id, entities, merge=merge)
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command('stream-entities')
@click.option('-f', '--foreign-id', help="foreign_id of the collection")
@click.pass_context
def stream_entities(ctx, foreign_id):
    """Load entities from the server and print them to stdout."""
    stdout = click.get_text_stream('stdout')
    api = ctx.obj["api"]
    try:
        include = ['id', 'schema', 'properties']
        collection = api.get_collection_by_foreign_id(foreign_id)
        if collection is None:
            raise click.BadParameter("Collection %r not found!" % foreign_id)
        for entity in api.stream_entities(collection_id=collection.get('id'),
                                          include=include,
                                          decode_json=False):
            stdout.write(json.dumps(entity))
            stdout.write('\n')
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


if __name__ == "__main__":
    cli()
