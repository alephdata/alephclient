import logging
import concurrent.futures

from alephclient.tasks.util import load_collection, to_path
from alephclient.errors import AlephException

log = logging.getLogger(__name__)


def _get_foreign_id(root_path, path):
    if path == root_path:
        if path.is_dir():
            return
        return path.name
    if root_path in path.parents:
        return str(path.relative_to(root_path))


def _upload_path(api, collection_id, languages, root_path, path):
    foreign_id = _get_foreign_id(root_path, path)
    if foreign_id is None:
        return
    metadata = {
        'foreign_id': foreign_id,
        'languages': languages,
        'file_name': path.name,
    }
    log.info('Upload [%s]: %s', collection_id, foreign_id)
    parent_id = _get_foreign_id(root_path, path.parent)
    if parent_id is not None:
        metadata['parent'] = {'foreign_id': parent_id}
    file_path = None if path.is_dir() else path
    api.ingest_upload(collection_id, file_path, metadata=metadata)


def _crawl_path(api, collection_id, languages, root_path, path):
    try:
        _upload_path(api, collection_id, languages, root_path, path)
    except AlephException as exc:
        log.error(exc.message)
    except Exception:
        log.exception('Failed [%s]: %s', collection_id, path)


def _get_path_list(root_path, path_list):
    if root_path.is_dir():
        for child in root_path.iterdir():
            path_list.append(child)
            path_list = _get_path_list(child, path_list)
    return path_list


def crawl_dir(api, path, foreign_id, config):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    root_path = to_path(path)
    collection_id = load_collection(api, foreign_id, config)
    languages = config.get('languages', [])
    paths = [root_path,]
    paths = _get_path_list(root_path, paths)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for path in paths:
            executor.submit(
                _crawl_path, api, collection_id, languages, root_path, path
            )
