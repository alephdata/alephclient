import logging

from alephclient.tasks.util import load_collection, to_path

log = logging.getLogger(__name__)


SKIP_LIST = [
    '.crawllog'
]


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


def _update_crawl_log(crawl_log_path, file_path):
    with open(crawl_log_path, 'a') as fp:
        fp.write(str(file_path))
        fp.write('\n')


def _crawl_path(api, collection_id, languages, root_path, path, config):
    try:
        if str(path) in config['skip_list']:
            return
        if config.get('resume'):
            if str(path) not in config.get('crawled_files'):
                _upload_path(api, collection_id, languages, root_path, path)
                _update_crawl_log(config['crawl_log'], path)
            else:
                log.info('Skipped %s', path)
        else:
            _upload_path(api, collection_id, languages, root_path, path)
            _update_crawl_log(config['crawl_log'], path)
        if not path.is_dir():
            return

        for child in path.iterdir():
            _crawl_path(
                api, collection_id, languages, root_path, child, config
            )
    except Exception:
        log.exception('Failed [%s]: %s', collection_id, path)


def _get_crawl_log(path):
    file_path = str((path / '.crawllog').resolve())
    with open(file_path, 'a+') as fp:
        fp.seek(0)
        crawled_files = [line.strip() for line in fp]
    return file_path, crawled_files


def crawl_dir(api, path, foreign_id, config):
    """Crawl a directory and upload its content to a collection"""
    path = to_path(path)
    config['crawl_log'], config['crawled_files'] = _get_crawl_log(path)
    config['skip_list'] = [
        str((path / filename).absolute()) for filename in SKIP_LIST
    ]
    collection_id = load_collection(api, foreign_id, config)
    languages = config.get('languages', [])
    _crawl_path(api, collection_id, languages, path, path, config)
