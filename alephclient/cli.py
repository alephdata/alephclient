import json
import click
import logging
from pathlib import Path

from alephclient import settings
from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.crawldir import crawl_dir
from alephclient.fetchdir import fetch_collection, fetch_entity

log = logging.getLogger(__name__)


def _get_id_from_foreign_key(api, foreign_id):
    collection = api.get_collection_by_foreign_id(foreign_id)
    if collection is None:
        raise click.ClickException("Collection does not exist.")
    return collection.get("id")


def _write_result(stream, result):
    for data in result:
        stream.write(json.dumps(data))
        stream.write("\n")


@click.group()
@click.option(
    "--host", default=settings.HOST, metavar="HOST", help="Aleph API host URL"
)
@click.option(
    "--api-key",
    default=settings.API_KEY,
    metavar="KEY",
    help="Aleph API key for authentication",
)
@click.option(
    "-r",
    "--retries",
    type=int,
    default=settings.MAX_TRIES,
    help="retries upon server failure",
)
@click.pass_context
def cli(ctx, host, api_key, retries):
    """API client for Aleph API"""
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpstream").setLevel(logging.WARNING)
    if not host:
        raise click.BadParameter("Missing Aleph host URL")
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj["api"] = AlephAPI(host, api_key, retries=retries)


@cli.command()
@click.option("--casefile", is_flag=True, default=False, help="handle as case file")
@click.option(
    "-i",
    "--noindex",
    is_flag=True,
    default=False,
    help="do not index documents after ingest",
)
@click.option(
    "-d",
    "--nojunk",
    is_flag=True,
    default=False,
    help="skip dot files, Thumbs.db and other files that are junk in most cases",
)
@click.option(
    "-l",
    "--language",
    multiple=True,
    help="language hint: 2-letter language code (ISO 639)",
)
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.argument("path", type=click.Path(exists=True))
@click.pass_context
def crawldir(ctx, path, foreign_id, language=None, casefile=False, noindex=False, nojunk=False):
    """Crawl a directory recursively and upload the documents in it to a
    collection."""
    try:
        config = {"languages": language, "casefile": casefile}
        api = ctx.obj["api"]
        crawl_dir(api, path, foreign_id, config, index=not noindex, nojunk=nojunk)
    except AlephException as exc:
        raise click.ClickException(str(exc))


@cli.command()
@click.option("-f", "--foreign-id", help="foreign_id of the collection")
@click.option("-e", "--entity-id", help="id of the root entity to download")
@click.option(
    "-p",
    "--prefix",
    type=click.Path(writable=True),
    help="destination path for the download",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="overwrite existing files",
)
@click.pass_context
def fetchdir(ctx, foreign_id, prefix=None, entity_id=None, overwrite=False):
    """Recursively download the contents of an Aleph entity or collection and rebuild
    them as a folder tree."""
    try:
        api = ctx.obj["api"]
        if entity_id is not None:
            fetch_entity(api, prefix, entity_id, overwrite=overwrite)
        elif foreign_id is not None:
            fetch_collection(api, prefix, foreign_id, overwrite=overwrite)
        else:
            msg = "Please specify either a foreign_id or entity_id"
            raise click.ClickException(msg)
    except AlephException as exc:
        raise click.ClickException(str(exc))


@cli.command("reingest")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option(
    "--index",
    is_flag=True,
    default=False,
    help="index documents as they are being processed",
)
@click.pass_context
def reingest_collection(ctx, foreign_id, index=False):
    """Trigger a re-ingest on all the documents in the collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.reingest_collection(collection_id, index=index)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command("reindex")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option(
    "--flush", is_flag=True, default=False, help="flush entities before indexing"
)
@click.pass_context
def reindex_collection(ctx, foreign_id, flush=False):
    """Trigger a re-index of all the entities in the collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.reindex_collection(collection_id, flush=flush)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command("delete")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option("--sync", is_flag=True, default=False, help="wait for delete to complete")
@click.pass_context
def delete_collection(ctx, foreign_id, sync=False):
    """Delete a collection and all its contents."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.delete_collection(collection_id, sync=sync)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command("flush")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option("--sync", is_flag=True, default=False, help="wait for delete to complete")
@click.pass_context
def flush_collection(ctx, foreign_id, sync=False):
    """Delete a all the contents of a collection."""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        api.flush_collection(collection_id, sync=sync)
    except AlephException as exc:
        raise click.ClickException(exc.message)


@cli.command("write-entities")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option(
    "-e", "--entityset", "entityset_id", help="add entities to the given entity set"
)
@click.option(
    "--force", is_flag=True, default=False, help="continue after server errors"
)
@click.option(
    "--unsafe", is_flag=True, default=False, help="disable server-side validation"
)
@click.pass_context
def write_entities(
    ctx, infile, foreign_id, entityset_id=None, force=False, unsafe=False
):
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
                    log.info("[%s] Bulk load entities: %s...", foreign_id, count)
                yield json.loads(line)

        api.write_entities(
            collection.get("id"),
            read_json_stream(infile),
            unsafe=unsafe,
            force=force,
            entityset_id=entityset_id,
        )
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command("stream-entities")
@click.option("-o", "--outfile", type=click.File("w"), default="-")  # noqa
@click.option("-s", "--schema", multiple=True, default=[])  # noqa
@click.option("-f", "--foreign-id", help="foreign_id of the collection")
@click.option(
    "-p",
    "--publisher",
    is_flag=True,
    default=False,
    help="Add publisher info from collection context",
)
@click.pass_context
def stream_entities(ctx, outfile, schema, foreign_id, publisher):
    """Load entities from the server and print them to stdout."""
    api = ctx.obj["api"]
    try:
        include = ["id", "schema", "properties"]
        collection = api.get_collection_by_foreign_id(foreign_id)
        if collection is None:
            raise click.BadParameter("Collection %r not found!" % foreign_id)
        res = api.stream_entities(
            collection=collection, include=include, schema=schema, publisher=publisher
        )
        _write_result(outfile, res)
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command("entitysets")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-f", "--foreign-id", default=None, help="foreign_id of the collection")
@click.option("-t", "--type", "type_", default=None, help="entity set type")
@click.pass_context
def entitysets(ctx, outfile, foreign_id, type_):
    """Stream all entity sets."""
    api = ctx.obj["api"]
    try:
        collection_id = None
        if foreign_id is not None:
            collection_id = _get_id_from_foreign_key(api, foreign_id)
        res = api.entitysets(collection_id=collection_id, set_types=type_)
        _write_result(outfile, res)
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command("entitysetitems")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.argument("entityset_id")
@click.pass_context
def entitysetitems(ctx, outfile, entityset_id):
    """Stream all entity sets."""
    api = ctx.obj["api"]
    try:
        res = api.entitysetitems(entityset_id=entityset_id)
        _write_result(outfile, res)
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


@cli.command("make-list")
@click.option("-f", "--foreign-id", required=True, help="foreign_id of the collection")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.argument("label")
@click.option("-s", "--summary", type=str)
@click.pass_context
def make_list(ctx, foreign_id, outfile, label, summary):
    """Create a list"""
    api = ctx.obj["api"]
    try:
        collection_id = _get_id_from_foreign_key(api, foreign_id)
        res = api.create_entityset(collection_id, "list", label, summary)
        outfile.write(res.get("id"))
    except AlephException as exc:
        raise click.ClickException(exc.message)
    except BrokenPipeError:
        raise click.Abort()


if __name__ == "__main__":
    cli()
