import os
import logging
from banal import decode_path

from alephclient.tasks.util import load_collection

log = logging.getLogger(__name__)


def _upload_path(api, collection_id, languages, root_path, path):
    relative_path = os.path.relpath(path, root_path)
    if relative_path == '.':
        if os.path.isdir(path):
            return
        relative_path = os.path.basename(path)
    metadata = {
        'foreign_id': relative_path,
        'languages': languages,
        'file_name': os.path.basename(path),
    }
    parent_path = os.path.dirname(relative_path)
    log.info('Upload [%s]: %s', collection_id, relative_path)
    if len(parent_path):
        metadata['parent'] = {'foreign_id': parent_path}
    file_path = None if os.path.isdir(path) else path
    api.ingest_upload(collection_id, file_path, metadata=metadata)


def _crawl_path(api, collection_id, languages, root_path, path):
    try:
        path = decode_path(path)
        _upload_path(api, collection_id, languages, root_path, path)
        if os.path.isdir(path):
            for child in os.listdir(path):
                try:
                    child = os.path.join(path, decode_path(child))
                    _crawl_path(api,
                                collection_id,
                                languages,
                                root_path,
                                child)
                except UnicodeDecodeError:
                    log.warning("Skip child: %r", child)
    except Exception:
        log.exception('Upload failed.')


def crawl_dir(api, path, foreign_id, config):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    path = decode_path(os.path.abspath(os.path.normpath(path)))
    collection_id = load_collection(api, foreign_id, config)
    languages = config.get('languages', [])
    _crawl_path(api, collection_id, languages, path, path)
