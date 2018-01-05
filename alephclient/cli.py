import os

import click

from .api import AlephAPI
from .tasks import crawl_dir


@click.group()
@click.option('--api-base-url', help="Aleph API address",
              default="http://127.0.0.1:5000/api/2/")
@click.option("--api-key", help="Aleph API key for authentication",
              required=True)
@click.pass_context
def cli(ctx, api_base_url, api_key):
    """API client for Aleph API"""
    ctx.obj["api"] = AlephAPI(api_base_url, api_key)


@cli.command()
@click.option('--language', multiple=True, help="language hint")
@click.option('--country', multiple=True, help="country hint")
@click.option('--foreign-id', required=True,
              help="foreign_id of the collection to use or create")
@click.option('--category',
              help="category of the collection if creating a new one")
@click.argument('path', help="path of the directory to crawl")
@click.pass_context
def crawldir(ctx, path, foreign_id, category=None,
             language=None, country=None):
    """Crawl a directory recursively and upload the documents in it to a
    collection."""
    if not os.path.isdir(path):
        raise click.BadParameter(
            "Path needs to be a directory", param_hint="path"
        )
    crawl_dir(ctx.obj["api"], path, foreign_id, category, language, country)


if __name__ == "__main__":
    cli(obj={})
