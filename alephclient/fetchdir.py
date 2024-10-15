import logging
import requests
from pathlib import Path
from pprint import pprint  # noqa
from typing import Optional, Dict

from alephclient.api import AlephAPI

log = logging.getLogger(__name__)


def _fix_path(prefix: Optional[str]):
    if prefix is None:
        return Path.cwd().resolve()
    return Path(prefix).resolve()


def _get_filename(entity):
    filenames = entity.get("properties", {}).get("fileName", [])
    if len(filenames):
        return max(filenames, key=len)
    return entity.get("id")


def fetch_archive(url: str, path: Path):
    with open(path, "wb") as fh:
        res = requests.get(url)
        for chunk in res.iter_content(chunk_size=512 * 1024):
            if chunk:  # filter out keep-alive new chunks
                fh.write(chunk)


def fetch_object(api: AlephAPI, path: Path, entity: Dict, overwrite: bool = False):
    file_name = _get_filename(entity)
    path.mkdir(exist_ok=True, parents=True)
    object_path = path.joinpath(file_name)
    url = entity.get("links", {}).get("file")
    if url is not None:
        # Skip existing files after checking file size:
        if not overwrite and object_path.exists():
            for file_size in entity.get("properties", {}).get("fileSize", []):
                if int(file_size) == object_path.stat().st_size:
                    log.info("Skip [%s]: %s", path, file_name)
                    return

        log.info("Fetch [%s]: %s", path, file_name)
        return fetch_archive(url, object_path)

    filters = [("properties.parent", entity.get("id"))]
    results = api.search("", filters=filters, schemata="Document")
    log.info("Directory [%s]: %s (%d children)", path, file_name, len(results))
    for entity in results:
        fetch_object(api, object_path, entity, overwrite=overwrite)


def fetch_entity(
    api: AlephAPI, prefix: Optional[str], entity_id: str, overwrite: bool = False
):
    entity = api.get_entity(entity_id)
    return fetch_object(api, _fix_path(prefix), entity, overwrite=overwrite)


def fetch_collection(
    api: AlephAPI, prefix: Optional[str], foreign_id: str, overwrite: bool = False
):
    path = _fix_path(prefix)
    collection = api.get_collection_by_foreign_id(foreign_id)
    if collection is None:
        return
    filters = [("collection_id", collection.get("id"))]
    params = {"empty:properties.parent": "true"}
    results = api.search("", filters=filters, schemata="Document", params=params)
    label = collection.get("label")
    log.info("Dataset [%s]: %s (%d children)", path, label, len(results))
    for entity in results:
        fetch_object(api, path, entity, overwrite=overwrite)
