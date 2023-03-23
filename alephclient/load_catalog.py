import json
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from banal import ensure_list

from alephclient.api import AlephAPI

log = logging.getLogger(__name__)

EntityData = Dict[str, Any]
Loader = Tuple[str, Generator[EntityData, None, None]]

MIME_TYPE = "application/json+ftm"


def stream_resource(url: str, foreign_id: str) -> Generator[EntityData, None, None]:
    res = requests.get(url, stream=True)
    if not res.ok:
        log.error(f"[{foreign_id}] {res.status_code}")
        return

    for ix, data in enumerate(res.iter_lines()):
        if ix and ix % 1000 == 0:
            log.info("[%s] Bulk load entities: %s...", foreign_id, ix)
        yield json.loads(data)


def load_catalog(
    api: AlephAPI,
    url: str,
    exclude_datasets: Optional[List[str]] = [],
    include_datasets: Optional[List[str]] = [],
    frequency: Optional[str] = None,
) -> Generator[Loader, None, None]:
    res = requests.get(url)
    if not res.ok:
        raise requests.HTTPError(f"Fetch catalog failed: {res.status_code}")

    catalog = res.json()
    for dataset in catalog["datasets"]:
        foreign_id = dataset["name"]

        if "type" in dataset and dataset["type"] == "collection":
            continue
        if exclude_datasets and foreign_id in exclude_datasets:
            continue
        if include_datasets and foreign_id not in include_datasets:
            continue

        aleph_collection = api.get_collection_by_foreign_id(foreign_id)
        data = {
            "label": dataset["title"],
            "summary": (
                dataset.get("description", "") + "\n\n" + dataset.get("summary", "")
            ).strip(),
            "publisher": dataset.get("publisher", {}).get("name"),
            "publisher_url": dataset.get("publisher", {}).get("url"),
            "countries": ensure_list(dataset.get("publisher", {}).get("country")),
            "data_url": dataset.get("data", {}).get("url"),
            "category": dataset.get("category", "other"),
        }
        if "frequency" in dataset or frequency is not None:
            data["frequency"] = dataset.get("frequency", frequency)

        if aleph_collection is not None:
            log.info("[%s] Updating collection metadata ..." % foreign_id)
            data.pop(
                "category", None
            )  # don't overwrite existing (probably user changed) category
            aleph_collection = api.update_collection(
                aleph_collection["collection_id"], data
            )
        else:
            log.info("[%s] Creating collection ..." % foreign_id)
            aleph_collection = api.create_collection(
                {**data, **{"foreign_id": dataset["name"]}}
            )

        for resource in ensure_list(dataset.get("resources")):
            if resource["mime_type"] == MIME_TYPE:
                loader = stream_resource(resource["url"], foreign_id)
                if loader is not None:
                    yield aleph_collection["collection_id"], loader
