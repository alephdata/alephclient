import os
import logging


log = logging.getLogger('alephclient')


def crawl_dir(api, path, foreign_id, category, language=None, country=None):
    path = os.path.abspath(os.path.normpath(path))
    path_name = os.path.basename(path)
    collections = api.filter_collections(filters=[("foreign_id", foreign_id)])
    if not collections:
        collection_data = {
            'foreign_id': foreign_id,
            'label': path_name,
            'managed': True,
            'category': category
        }
        if language is not None:
            collection_data["languages"] = [language]
        if country is not None:
            collection_data["countries"] = [country]
        collection = api.create_collection(data=collection_data)
    else:
        collection = collections[0]
    log.info('Crawling %r to %r...', path, foreign_id)
    # Crawl dir and add documents to aleph
    for dirpath, subdirs, files in os.walk(path):
        for f in files:
            full_file_path = os.path.join(dirpath, f)
            relative_file_path = os.path.relpath(full_file_path, path)
            api.ingest_upload(
                collection["id"], full_file_path, relative_file_path
            )
