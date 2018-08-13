import logging

from alephclient.tasks.util import load_collection, load_config_file

log = logging.getLogger(__name__)


def bulk_load(api, mapping_file):
    data = load_config_file(mapping_file)
    for foreign_id, config in data.items():
        collection_id = load_collection(api, foreign_id, config)
        log.info("Bulk mapping collection ID: %s", collection_id)
        api.map_collection(collection_id, data)
