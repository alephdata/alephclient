from pathlib import Path
from pprint import pprint  # noqa
from banal import clean_dict  # type: ignore
from typing import Optional

from alephclient import settings
from alephclient.api import AlephAPI
from alephclient.util import backoff
from alephclient.errors import AlephException
from memorious.core import get_rate_limit  # type: ignore


def aleph_emit(context, data):
    api = get_api(context)
    if api is None:
        return
    collection_id = get_collection_id(context, api)
    content_hash = data.get('content_hash')
    source_url = data.get('source_url', data.get('url'))
    foreign_id = data.get('foreign_id', data.get('request_id', source_url))
    if context.skip_incremental(collection_id, foreign_id, content_hash):
        context.log.info("Skip aleph upload: %s", foreign_id)
        return

    meta = {
        'crawler': context.crawler.name,
        'foreign_id': foreign_id,
        'source_url': source_url,
        'title': data.get('title'),
        'author': data.get('author'),
        'file_name': data.get('file_name'),
        'retrieved_at': data.get('retrieved_at'),
        'modified_at': data.get('modified_at'),
        'published_at': data.get('published_at'),
        'headers': data.get('headers', {})
    }

    languages = context.params.get('languages')
    meta['languages'] = data.get('languages', languages)
    countries = context.params.get('countries')
    meta['countries'] = data.get('countries', countries)
    mime_type = context.params.get('mime_type')
    meta['mime_type'] = data.get('mime_type', mime_type)

    if data.get('parent_foreign_id'):
        meta['parent'] = {'foreign_id': data.get('parent_foreign_id')}

    meta = clean_dict(meta)
    # pprint(meta)
    label = meta.get('file_name', meta.get('source_url'))
    context.log.info("Upload: %s", label)
    with context.load_file(content_hash) as fh:
        if fh is None:
            return
        file_path = Path(fh.name).resolve()

        for try_number in range(api.retries):
            rate = settings.MEMORIOUS_RATE_LIMIT
            rate_limit = get_rate_limit('aleph', limit=rate)
            rate_limit.comply()
            try:
                res = api.ingest_upload(collection_id, file_path, meta)
                document_id = res.get('id')
                context.log.info("Aleph document entity ID: %s", document_id)
                data['aleph_id'] = document_id
                data['aleph_document'] = meta
                data['aleph_collection_id'] = collection_id
                context.emit(data=data, optional=True)
                return
            except AlephException as ae:
                if try_number > api.retries or not ae.transient:
                    context.emit_warning("Error: %s" % ae)
                    return
                backoff(ae, try_number)


def get_api(context) -> Optional[AlephAPI]:
    if not settings.HOST:
        context.log.warning("No $ALEPHCLIENT_HOST, skipping upload...")
        return None
    if not settings.API_KEY:
        context.log.warning("No $ALEPHCLIENT_API_KEY, skipping upload...")
        return None

    session_id = 'memorious:%s' % context.crawler.name
    return AlephAPI(settings.HOST,
                    settings.API_KEY,
                    session_id=session_id)


def get_collection_id(context, api):
    if hasattr(context.stage, '_aleph_cid'):
        return context.stage._aleph_cid
    foreign_id = context.get('collection', context.crawler.name)
    config = {'label': context.crawler.description}
    collection = api.load_collection_by_foreign_id(foreign_id, config=config)
    context.stage._aleph_cid = collection.get('id')
    return context.stage._aleph_cid
