import uuid
import json
import logging
import pkg_resources
from itertools import count
from banal import ensure_list  # type: ignore
from six.moves.urllib.parse import urlencode, urljoin
from requests import Session, RequestException
from requests_toolbelt import MultipartEncoder  # type: ignore
from pathlib import Path
from typing import Optional, Dict, List, Generator, Iterable

from alephclient import settings
from alephclient.errors import AlephException
from alephclient.util import backoff, prop_push

log = logging.getLogger(__name__)
MIME = 'application/octet-stream'
VERSION = pkg_resources.get_distribution('alephclient').version


class AlephAPI(object):

    def __init__(self,
                 host: Optional[str]=settings.HOST,
                 api_key: Optional[str]=settings.API_KEY,
                 session_id: Optional[str]=None,
                 retries: int=settings.MAX_TRIES):

        if not host:
            raise AlephException('No host environment variable found')
        if not api_key:
            raise AlephException('No API key environment variable found')
        self.base_url = urljoin(host, '/api/2/')
        self.retries = retries
        session_id = session_id or str(uuid.uuid4())
        self.session: Session = Session()
        self.session.headers['X-Aleph-Session'] = session_id
        self.session.headers['User-Agent'] = 'alephclient/%s' % VERSION
        if api_key is not None:
            self.session.headers['Authorization'] = 'ApiKey %s' % api_key

    def _make_url(self, path: str, query: Optional[str]=None,
                  filters: Optional[List]=None, **params):
        """Construct the target url from given args"""
        url = self.base_url + path
        if query:
            params["q"] = query
        if filters:
            for key, val in filters:
                params["filter:" + key] = val
        if len(params):
            url = url + '?' + urlencode(params)
        return url

    def _request(self, method: str, url: str, **kwargs) -> Dict:
        """A single point to make the http requests.

        Having a single point to make all requests let's us set headers, manage
        successful and failed responses and possibly manage session etc
        conviniently in a single place.
        """
        try:
            response = self.session.request(method=method, url=url, **kwargs)
            response.raise_for_status()
        except RequestException as exc:
            raise AlephException(exc)

        if len(response.text):
            return response.json()
        else:
            # NOTE: Raise exception?
            return {}

    def search(self, query: str, schema: Optional[str]=None,
               schemata: Optional[str]=None, filters: Optional[List]=None):
        """Conduct a search and return the search results."""
        filters_list: List = ensure_list(filters)
        if schema is not None:
            filters_list.append(('schema', schema))
        if schemata is not None:
            filters_list.append(('schemata', schemata))
        if schema is None and schemata is None:
            filters_list.append(('schemata', 'Thing'))
        url = self._make_url('entities', query=query, filters=filters_list)
        return APIResultSet(self, url)

    def get_collection(self, collection_id: str) -> Dict:
        """Get a single collection by ID (not foreign ID!)."""
        url = self._make_url(f'collections/{collection_id}')
        return self._request('GET', url)

    def get_entity(self, entity_id: str) -> Dict:
        """Get a single entity by ID."""
        url = self._make_url(f'entities/{entity_id}')
        return self._request('GET', url)

    def get_collection_by_foreign_id(self, foreign_id: str) -> Optional[Dict]:
        """Get a dict representing a collection based on its foreign ID."""
        if foreign_id is None:
            return None
        filters = [('foreign_id', foreign_id)]
        for coll in self.filter_collections(filters=filters):
            return coll
        return None

    def load_collection_by_foreign_id(self, foreign_id: str,
                                      config: Optional[Dict]=None) -> Dict:
        """Get a collection by its foreign ID, or create one."""
        collection = self.get_collection_by_foreign_id(foreign_id)
        if collection is not None:
            return collection

        config = config or {}
        collection = self.create_collection({
            'foreign_id': foreign_id,
            'label': config.get('label', foreign_id),
            'casefile': config.get('casefile', False),
            'category': config.get('category', 'other'),
            'languages': config.get('languages', []),
            'summary': config.get('summary', ''),
        })
        return collection

    def filter_collections(self, query: str=None,
                           filters: Optional[List]=None, **kwargs):
        """Filter collections for the given query and/or filters.

        params
        ------
        query: query string
        filters: list of key, value pairs to filter collections
        kwargs: extra arguments for api call such as page, limit etc
        """
        if not query and not filters:
            raise ValueError("One of query or filters is required")
        url = self._make_url("collections", query=query,
                             filters=filters, **kwargs)
        return APIResultSet(self, url)

    def create_collection(self, data: Dict) -> Dict:
        """Create a collection from the given data.

        params
        ------
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        url = self._make_url("collections")
        return self._request("POST", url, json=data)

    def update_collection(self, collection_id: str, data: Dict) -> Dict:
        """Update an existing collection using the given data.

        params
        ------
        collection_id: id of the collection to update
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        url = self._make_url(f"collections/{collection_id}")
        return self._request("PUT", url, json=data)

    def map_collection(self, collection_id: str, mapping: Dict):
        """Run a bulk entity data mapping on a collection.

        params
        ------
        collection_id: id of the collection to update
        mapping: dict with the entity bulk load mapping.
        """
        url = self._make_url(f"collections/{collection_id}/mapping")
        return self._request("PUT", url, json=mapping)

    def stream_entities(self, collection_id: str=None,
                        include: Optional[List]=None,
                        schema: Optional[str]=None) -> Generator[Dict, None, None]:
        """Iterate over all entities in the given collection.

        params
        ------
        collection_id: id of the collection to stream
        include: an array of fields from the index to include.
        """
        url = self._make_url('entities/_stream')
        if collection_id is not None:
            url = f"collections/{collection_id}/_stream"
            url = self._make_url(url)
        params = {'include': include, 'schema': schema}
        try:
            res = self.session.get(url, params=params, stream=True)
            res.raise_for_status()
            for entity in res.iter_lines():
                if isinstance(entity, bytes):
                    entity = entity.decode('utf-8')
                entity = json.loads(entity)
                aleph_url = 'entities/%s' % entity.get('id')
                prop_push(entity, 'alephUrl', self._make_url(aleph_url))
                yield entity
        except RequestException as exc:
            raise AlephException(exc)

    def _bulk_chunk(self, collection_id: str, chunk: List, force: bool=False,
                    unsafe: bool=False):
        for attempt in count(1):
            url = self._make_url(f"collections/{collection_id}/_bulk")
            params = {'unsafe': unsafe}
            try:
                response = self.session.post(url, json=chunk, params=params)
                response.raise_for_status()
                return
            except RequestException as exc:
                ae = AlephException(exc)
                if not ae.transient or attempt > self.retries:
                    if not force:
                        raise ae
                    log.error(ae)
                    return
                backoff(ae, attempt)

    def write_entities(self, collection_id: str, entities: Iterable,
                       chunk_size: int=1000, **kw):
        """Create entities in bulk via the API, in the given
        collection.

        params
        ------
        collection_id: id of the collection to use
        entities: an iterable of entities to upload
        """
        chunk = []
        for entity in entities:
            chunk.append(entity)
            if len(chunk) >= chunk_size:
                self._bulk_chunk(collection_id, chunk, **kw)
                chunk = []
        if len(chunk):
            self._bulk_chunk(collection_id, chunk, **kw)

    def match(self, entity: Dict, collection_ids: Optional[str]=None,
              url: str=None) -> Generator[List, None, None]:
        """Find similar entities given a sample entity."""
        params = {
            'collection_ids': ensure_list(collection_ids)
        }
        if url is None:
            url = self._make_url('match')
        try:
            response = self.session.post(url, json=entity, params=params)
            response.raise_for_status()
            for result in response.json().get('results', []):
                yield result
        except RequestException as exc:
            raise AlephException(exc)

    def linkages(self, context_ids: Optional[List]=None):
        """Stream all linkages within the given role contexts."""
        filters = [('context_id', c) for c in ensure_list(context_ids)]
        url = self._make_url('linkages', filters=filters)
        return APIResultSet(self, url)

    def ingest_upload(self, collection_id: str, file_path: Optional[Path]=None,
                      metadata: Optional[Dict]=None) -> Dict:
        """
        Create an empty folder in a collection or upload a document to it

        params
        ------
        collection_id: id of the collection to upload to
        file_path: path of the file to upload. None while creating folders
        metadata: dict containing metadata for the file or folders. In case of
        files, metadata contains foreign_id of the parent. Metadata for a
        directory contains foreign_id for itself as well as its parent and the
        name of the directory.
        """
        url = self._make_url("collections/{0}/ingest".format(collection_id))
        if not file_path or file_path.is_dir():
            data = {"meta": json.dumps(metadata)}
            return self._request("POST", url, data=data)

        for attempt in count(1):
            try:
                with file_path.open('rb') as fh:
                    # use multipart encoder to allow uploading very large files
                    m = MultipartEncoder(fields={
                        'meta': json.dumps(metadata),
                        'file': (file_path.name, fh, MIME)
                    })
                    headers = {'Content-Type': m.content_type}
                    return self._request("POST", url, data=m, headers=headers)
            except AlephException as ae:
                if not ae.transient or attempt > self.retries:
                    raise ae
                backoff(ae, attempt)
        return {}


class APIResultSet(object):

    def __init__(self, api: AlephAPI, url: str):
        self.api = api
        self.url = url
        self.current = 0
        self.result = self.api._request('GET', self.url)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= self.result.get('limit'):
            next_url = self.result.get('next')
            if next_url is None:
                raise StopIteration
            self.result = self.api._request('GET', next_url)
        try:
            res = self.result.get('results', [])[self.index]
        except IndexError:
            raise StopIteration
        self.current += 1
        return res

    next = __next__

    @property
    def index(self):
        return self.current - self.result.get('offset')

    def __len__(self):
        return self.result.get('total')

    def __repr__(self):
        return '<APIResultSet(%r, %r)>' % (self.url, len(self))
