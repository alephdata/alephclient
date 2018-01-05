import os
import logging

from normality import slugify


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
    # Crawl dir and add documents to aleph
    for dirpath, subdirs, files in os.walk(path):
        relative_path = os.path.relpath(dirpath, path)
        if relative_path == ".":
            parent_foreign_id = foreign_id
        else:
            parent_foreign_id = foreign_id + ":" + slugify(relative_path)
        metadata = {
            "parent": {"foreign_id": parent_foreign_id}
        }
        for f in files:
            full_file_path = os.path.join(dirpath, f)
            api.ingest_upload(collection["id"], full_file_path, metadata)
        for subdir in subdirs:
            dir_foreign_id = (
                foreign_id + ":" + slugify(os.path.join(relative_path, subdir))
            )
            metadata["foreign_id"] = dir_foreign_id
            metadata["file_name"] = subdir
            api.ingest_upload(collection["id"], metadata=metadata)
