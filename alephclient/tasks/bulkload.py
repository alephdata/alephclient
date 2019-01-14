import logging

from alephclient.util import load_config_file

log = logging.getLogger(__name__)


def bulk_load(api, mapping_file):
    data = load_config_file(mapping_file)
    for foreign_id, config in data.items():
        collection = api.load_collection_by_foreign_id(foreign_id, config)
        collection_id = collection.get('id')
        log.info("Bulk mapping collection ID: %s", collection_id)
        api.map_collection(collection_id, data)
