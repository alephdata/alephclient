import logging
import click

from alephclient.api import AlephAPI
from alephclient.tasks import crawl_dir, bulk_load
from alephclient.errors import AlephException

log = logging.getLogger(__name__)


@click.group()
@click.option('--api-base-url', help="Aleph API address", envvar="ALEPH_HOST")
@click.option("--api-key", envvar="ALEPH_API_KEY",
              help="Aleph API key for authentication")
@click.pass_context
def cli(ctx, api_base_url, api_key):
    """API client for Aleph API"""
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    if not api_key:
        raise click.BadParameter("Missing API key", param_hint="api-key")
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj["api"] = AlephAPI(api_base_url, api_key)


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
    config = {
        'label': path,
        'languages': language,
        'casefile': casefile
    }
    api = ctx.obj["api"]
    crawl_dir(api, path, foreign_id, config)


@cli.command()
@click.argument('mapping_file')
@click.pass_context
def bulkload(ctx, mapping_file):
    """Trigger a load of structured entity data using the submitted mapping."""
    try:
        bulk_load(ctx.obj["api"], mapping_file)
    except AlephException as exc:
        log.error("Error: %s", exc.message)


if __name__ == "__main__":
    cli()
