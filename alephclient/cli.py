import json
import click
import logging

from alephclient import settings
from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.crawldir import crawl_dir

log = logging.getLogger(__name__)


def _get_id_from_foreign_key(api, foreign_id):
    collection = api.get_collection_by_foreign_id(foreign_id)
    if collection is None:
        raise click.ClickException("Collection does not exist.")
    return collection.get('id')


@click.group()
@click.option('--host', default=settings.HOST, metavar="HOST", help="Aleph API host URL")  # noqa
@click.option('--api-key', default=settings.API_KEY, metavar="KEY", help="Aleph API key for authentication")  # noqa
@click.option('-r', '--retries', type=int, default=settings.MAX_TRIES, help="retries upon server failure")  # noqa
@click.pass_context
def cli(ctx, host, api_key, retries):
    """API client for Aleph API"""
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    if not host:
        raise click.BadParameter('Missing Aleph host URL')
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj['api'] = AlephAPI(host, api_key, retries=retries)


@cli.command()
@click.option('--casefile', is_flag=True, default=False, help='handle as case file')  # noqa
@click.option('-i', '--noindex', is_flag=True, default=False, help='do not index documents after ingest')  # noqa
@click.option('-l', '--language', multiple=True, help="language hint: 2-letter language code (ISO 639)")  # noqa
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.argument('path', type=click.Path(exists=True))
@click.pass_context
def crawldir(ctx, path, foreign_id, language=None,
             casefile=False, noindex=False):
    """Crawl a directory recursively and upload the documents in it to a
    collection."""
    try:
        config = {
            'languages': language,
            'casefile': casefile
        }
        api = ctx.obj["api"]
        crawl_dir(api, path, foreign_id, config, index=not noindex)
    except AlephException as exc:
        raise click.ClickException(str(exc))


@cli.command('reingest')
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('--index', is_flag=True, default=False, help="index documents as they are being processed")  # noqa
@click.pass_context
def reingest_collection(ctx, foreign_id, index=False):
    """Trigger a re-ingest on all the documents in the collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.regingest_collection(collection_id, index=index)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command('reindex')
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('--flush', is_flag=True, default=False, help="flush entities before indexing")  # noqa
@click.pass_context
def reindex_collection(ctx, foreign_id, flush=False):
    """Trigger a re-index of all the entities in the collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.reindex_collection(collection_id, flush=flush)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command('delete')
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('--sync', is_flag=True, default=False, help="wait for delete to complete")  # noqa
@click.pass_context
def delete_collection(ctx, foreign_id, sync=False):
    """Delete a collection and all its contents."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.delete_collection(collection_id, sync=sync)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command('flush')
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('--sync', is_flag=True, default=False, help="wait for delete to complete")  # noqa
@click.pass_context
def flush_collection(ctx, foreign_id, sync=False):
    """Delete a all the contents of a collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.flush_collection(collection_id, sync=sync)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command('write-entities')
@click.option('-i', '--infile', type=click.File('r'), default='-')  # noqa
@click.option('-f', '--foreign-id', required=True, help="foreign_id of the collection")  # noqa
@click.option('--force', is_flag=True, default=False, help="continue after server errors")  # noqa
@click.option('--unsafe', is_flag=True, default=False, help="disable server-side validation")  # noqa
@click.pass_context
def write_entities(ctx, infile, foreign_id, force=False, unsafe=False):
    """Read entities from standard input and index them."""
    api = ctx.obj["api"]
    try:
        collection = api.load_collection_by_foreign_id(foreign_id)

        def read_json_stream(stream):
            count = 0
            while True:
                line = stream.readline()
                if not line:
                    return
                count += 1
                if count % 1000 == 0:
                    log.info("[%s] Bulk load entities: %s...",
                             foreign_id, count)
                yield json.loads(line)

        api.write_entities(collection.get('id'), read_json_stream(infile),
                           unsafe=unsafe, force=force)
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command('stream-entities')
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
@click.option('-s', '--schema', multiple=True, default=[])  # noqa
@click.option('-f', '--foreign-id', help="foreign_id of the collection")
@click.option('-p', '--publisher', is_flag=True, default=False, help="Add publisher info from collection context")  # noqa
@click.pass_context
def stream_entities(ctx, outfile, schema, foreign_id, publisher):
    """Load entities from the server and print them to stdout."""
    api = ctx.obj["api"]
    try:
        include = ['id', 'schema', 'properties']
        collection = api.get_collection_by_foreign_id(foreign_id)
        if collection is None:
            raise click.BadParameter("Collection %r not found!" % foreign_id)
        for entity in api.stream_entities(collection=collection,
                                          include=include,
                                          schema=schema,
                                          publisher=publisher):
            outfile.write(json.dumps(entity))
            outfile.write('\n')
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command('linkages')
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
@click.option('-c', '--context', multiple=True, default=[])  # noqa
@click.pass_context
def linkages(ctx, outfile, context):
    """Stream all linkages within the given role contexts."""
    api = ctx.obj["api"]
    try:
        for entity in api.linkages(context_ids=context):
            outfile.write(json.dumps(entity))
            outfile.write('\n')
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


if __name__ == "__main__":
    cli()
