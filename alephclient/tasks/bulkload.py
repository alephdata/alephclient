import yaml


def _load_collection(api, foreign_id, config):
    collections = api.filter_collections(filters=[('foreign_id', foreign_id)])
    for collection in collections:
        return collection.get('id')

    data = {
        'foreign_id': foreign_id,
        'label': config.get('label', foreign_id),
        'managed': config.get('managed', True),
        'category': config.get('category', 'other'),
        'summary': config.get('summary', ''),
    }
    collection = api.create_collection(data)
    return collection.get('id')


def bulk_load(api, mapping_file):
    with open(mapping_file, 'r') as fh:
        data = yaml.load(fh)

    for foreign_id, config in data.items():
        collection_id = _load_collection(api, foreign_id, config)
        api.map_collection(collection_id, data)
