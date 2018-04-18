

def load_collection(api, foreign_id, config):
    collections = api.filter_collections(filters=[('foreign_id', foreign_id)])
    for collection in collections:
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
