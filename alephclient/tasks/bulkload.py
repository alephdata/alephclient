import logging

from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.util import load_config_file

log = logging.getLogger(__name__)


def bulk_load(api: AlephAPI, mapping_file: str):
    data = load_config_file(mapping_file)
    if not isinstance(data, dict):
        raise AlephException('mapping_file has to be a json dictionary')
    for foreign_id, config in data.items():
        collection = api.load_collection_by_foreign_id(foreign_id, config)
        collection_id = collection['id']
        log.info(f"Bulk mapping collection ID: {collection_id}")
        api.map_collection(collection_id, data)
