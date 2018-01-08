import os


def crawl_dir(api, path, foreign_id, category=None,
              language=None, country=None):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use. (new collection is created
    if doesn't exist already)
    category: category of the collection if a new one if to be created
    language: language hint for the documents
    country: country hint for the documents
    """
    path = os.path.abspath(os.path.normpath(path))
    path_name = os.path.basename(path)
    collections = api.filter_collections(filters=[("foreign_id", foreign_id)])
    if not collections:
        collection_data = {
            'foreign_id': foreign_id,
            'label': path_name,
            'managed': True,
            'category': category or "other"
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
            parent_foreign_id = os.path.join(foreign_id, relative_path)
        for f in files:
            full_file_path = os.path.join(dirpath, f)
            file_name = os.path.basename(full_file_path)
            metadata = {
                "parent": {"foreign_id": parent_foreign_id},
                "foreign_id": os.path.join(parent_foreign_id, file_name),
                "file_name": file_name,
            }
            api.ingest_upload(collection["id"], full_file_path, metadata)
        for subdir in subdirs:
            dir_foreign_id = os.path.join(parent_foreign_id, subdir)
            metadata = {
                "parent": {"foreign_id": parent_foreign_id},
                "foreign_id": dir_foreign_id,
                "file_name": subdir,
            }
            api.ingest_upload(collection["id"], metadata=metadata)
