import yaml

from alephclient.tasks.util import load_collection


def bulk_load(api, mapping_file):
    with open(mapping_file, 'r') as fh:
        data = yaml.load(fh)

    for foreign_id, config in data.items():
        collection_id = load_collection(api, foreign_id, config)
        api.map_collection(collection_id, data)
