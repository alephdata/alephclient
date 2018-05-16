try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path


def to_path(path):
    return Path(path).resolve()


def load_collection(api, foreign_id, config):
    collections = api.filter_collections(filters=[('foreign_id', foreign_id)])
    for collection in collections:
        casefile = collection.get('casefile')
        collection['casefile'] = config.get('casefile', casefile)
        languages = config.get('languages', [])
        if len(languages):
            collection['languages'] = languages
        api.update_collection(collection.get('id'), collection)
        return collection.get('id')

    data = {
        'foreign_id': foreign_id,
        'label': config.get('label', foreign_id),
        'casefile': config.get('casefile', False),
        'category': config.get('category', 'other'),
        'languages': config.get('languages', []),
        'summary': config.get('summary', ''),
    }
    collection = api.create_collection(data)
    return collection.get('id')
