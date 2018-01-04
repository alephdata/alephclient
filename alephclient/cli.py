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
    ctx.obj["api"] = AlephAPI(api_base_url, api_key)


@cli.command()
@click.option('--language', multiple=True)
@click.option('--country', multiple=True)
@click.option('--foreign-id', required=True)
@click.option('--category')
@click.argument('path')
@click.pass_context
def crawldir(ctx, path, foreign_id, category=None,
             language=None, country=None):
    if not os.path.isdir(path):
        raise click.BadParameter(
            "Path needs to be a directory", param_hint="path"
        )
    crawl_dir(ctx.obj["api"], path, foreign_id, category, language, country)


if __name__ == "__main__":
    cli(obj={})
