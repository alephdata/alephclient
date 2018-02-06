import os
import logging

import click

from .api import AlephAPI
from .tasks import crawl_dir


@click.group()
@click.option('--api-base-url', help="Aleph API address", envvar="ALEPH_HOST",
              default="http://127.0.0.1:5000/api/2/")
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
@click.option('--language', multiple=True, help="language hint")
@click.option('--foreign-id',
              required=True,
              help="foreign_id of the collection")
@click.argument('path')
@click.pass_context
def crawldir(ctx, path, foreign_id, language=None):
    """Crawl a directory recursively and upload the documents in it to a
    collection."""
    crawl_dir(ctx.obj["api"], path, foreign_id, language)


if __name__ == "__main__":
    cli()
