import uuid
import json
import logging
import pkg_resources
from itertools import count
from banal import ensure_list, ensure_dict
from six.moves.urllib.parse import urlencode, urljoin
from requests import Session, RequestException
from requests_toolbelt import MultipartEncoder  # type: ignore
from pathlib import Path
from typing import Optional, Dict, List, Iterator, Iterable

from alephclient import settings
from alephclient.errors import AlephException
from alephclient.util import backoff, prop_push

log = logging.getLogger(__name__)
MIME = 'application/octet-stream'
VERSION = pkg_resources.get_distribution('alephclient').version


class APIResultSet(object):

    def __init__(self, api: 'AlephAPI', url: str):
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
            item = self.result.get('results', [])[self.index]
        except IndexError:
            raise StopIteration
        self.current += 1
        return self._patch(item)

    next = __next__

    def _patch(self, item):
        return item

    @property
    def index(self):
        return self.current - self.result.get('offset')

    def __len__(self):
        return self.result.get('total')

    def __repr__(self):
        return '<APIResultSet(%r, %r)>' % (self.url, len(self))


class EntityResultSet(APIResultSet):

    def __init__(self, api: 'AlephAPI', url: str, publisher: bool):
        super(EntityResultSet, self).__init__(api, url)
        self.publisher = publisher

    def _patch(self, item):
        return self.api._patch_entity(item, self.publisher)


class LinkageResultSet(APIResultSet):

    def __init__(self, api: 'AlephAPI', url: str, publisher: bool):
        super(LinkageResultSet, self).__init__(api, url)
        self.publisher = publisher

    def _patch(self, item):
        entity = ensure_dict(item.get('entity'))
        item['entity'] = self.api._patch_entity(entity, self.publisher)
        return item


class AlephAPI(object):

    def __init__(self,
                 host: Optional[str] = settings.HOST,
                 api_key: Optional[str] = settings.API_KEY,
                 session_id: Optional[str] = None,
                 retries: int = settings.MAX_TRIES):

        if not host:
            raise AlephException('No host environment variable found')
        self.base_url = urljoin(host, '/api/2/')
        self.retries = retries
        session_id = session_id or str(uuid.uuid4())
        self.session: Session = Session()
        self.session.headers['X-Aleph-Session'] = session_id
        self.session.headers['User-Agent'] = 'alephclient/%s' % VERSION
        if api_key is not None:
            self.session.headers['Authorization'] = 'ApiKey %s' % api_key

    def _make_url(self, path: str, query: Optional[str] = None,
                  filters: Optional[List] = None, **params):
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

    def _patch_entity(self, entity: Dict,
                      publisher: bool,
                      collection: Optional[Dict] = None):
        """Add extra properties from context to the given entity."""
        properties: Dict = entity.get('properties', {})
        collection_: Dict = collection or entity.get('collection') or {}
        links: Dict = entity.get('links', {})
        api_url = links.get('self')
        if api_url is None:
            api_url = 'entities/%s' % entity.get('id')
            api_url = self._make_url(api_url)
        prop_push(properties, 'alephUrl', api_url)

        if publisher:
            # Context: setting the original publisher or collection
            # label can help make the data more traceable when merging
            # data from multiple sources.
            publisher_label = collection_.get('label')
            publisher_label = collection_.get('publisher', publisher_label)
            prop_push(properties, 'publisher', publisher_label)

            publisher_url = collection_.get('links', {}).get('ui')
            publisher_url = collection_.get('publisher_url', publisher_url)
            prop_push(properties, 'publisherUrl', publisher_url)

        entity['properties'] = properties
        return entity

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
        return {}

    def search(self, query: str, schema: Optional[str] = None,
               schemata: Optional[str] = None,
               filters: Optional[List] = None,
               publisher: bool = False) -> 'EntityResultSet':
        """Conduct a search and return the search results."""
        filters_list: List = ensure_list(filters)
        if schema is not None:
            filters_list.append(('schema', schema))
        if schemata is not None:
            filters_list.append(('schemata', schemata))
        if schema is None and schemata is None:
            filters_list.append(('schemata', 'Thing'))
        url = self._make_url('entities', query=query, filters=filters_list)
        return EntityResultSet(self, url, publisher)

    def get_collection(self, collection_id: str) -> Dict:
        """Get a single collection by ID (not foreign ID!)."""
        url = self._make_url(f'collections/{collection_id}')
        return self._request('GET', url)

    def reingest_collection(self, collection_id: str,
                            index: bool = False):
        """Re-ingest all documents in a collection."""
        url = self._make_url(f"collections/{collection_id}/reingest",
                             index=index)
        return self._request("POST", url)

    def reindex_collection(self, collection_id: str,
                           flush: bool = False,
                           sync: bool = False):
        """Re-index all entities in a collection."""
        url = self._make_url(f"collections/{collection_id}/reindex",
                             sync=sync, flush=flush)
        return self._request("POST", url)

    def delete_collection(self, collection_id: str,
                          sync: bool = False):
        """Delete a collection by ID"""
        url = self._make_url(f"collections/{collection_id}", sync=sync)
        return self._request("DELETE", url)

    def flush_collection(self, collection_id: str,
                         sync: bool = False):
        """Empty all contents from a collection by ID"""
        url = self._make_url(f"collections/{collection_id}", sync=sync,
                             keep_metadata=True)
        return self._request("DELETE", url)

    def get_entity(self, entity_id: str, publisher: bool = False) -> Dict:
        """Get a single entity by ID."""
        url = self._make_url(f'entities/{entity_id}')
        entity = self._request('GET', url)
        return self._patch_entity(entity, publisher)

    def get_collection_by_foreign_id(self, foreign_id: str) -> Optional[Dict]:
        """Get a dict representing a collection based on its foreign ID."""
        if foreign_id is None:
            return None
        filters = [('foreign_id', foreign_id)]
        for coll in self.filter_collections(filters=filters):
            return coll
        return None

    def load_collection_by_foreign_id(self, foreign_id: str,
                                      config: Optional[Dict] = None
                                      ) -> Dict:
        """Get a collection by its foreign ID, or create one. Setting clear
        will clear any found collection."""
        collection = self.get_collection_by_foreign_id(foreign_id)
        if collection is not None:
            return collection

        config_: Dict = ensure_dict(config)
        return self.create_collection({
            'foreign_id': foreign_id,
            'label': config_.get('label', foreign_id),
            'casefile': config_.get('casefile', False),
            'category': config_.get('category', 'other'),
            'languages': config_.get('languages', []),
            'summary': config_.get('summary', ''),
        })

    def filter_collections(self, query: str = None,
                           filters: Optional[List] = None,
                           **kwargs) -> 'APIResultSet':
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

    def update_collection(self, collection_id: str, data: Dict,
                          sync: bool = False) -> Dict:
        """Update an existing collection using the given data.

        params
        ------
        collection_id: id of the collection to update
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        url = self._make_url(f"collections/{collection_id}", sync=sync)
        return self._request("PUT", url, json=data)

    def stream_entities(self, collection: Optional[Dict] = None,
                        include: Optional[List] = None,
                        schema: Optional[str] = None,
                        publisher: bool = False) -> Iterator[Dict]:
        """Iterate over all entities in the given collection.

        params
        ------
        collection_id: id of the collection to stream
        include: an array of fields from the index to include.
        """
        url = self._make_url('entities/_stream')
        if collection is not None:
            collection_id = collection.get('id')
            url = f"collections/{collection_id}/_stream"
            url = self._make_url(url)
        params = {'include': include, 'schema': schema}
        try:
            res = self.session.get(url, params=params, stream=True)
            res.raise_for_status()
            for entity in res.iter_lines():
                entity = json.loads(entity)
                yield self._patch_entity(entity,
                                         publisher=publisher,
                                         collection=collection)
        except RequestException as exc:
            raise AlephException(exc)

    def _bulk_chunk(self, collection_id: str, chunk: List, force: bool = False,
                    unsafe: bool = False):
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
                       chunk_size: int = 1000, **kw):
        """Create entities in bulk via the API, in the given
        collection.

        params
        ------
        collection_id: id of the collection to use
        entities: an iterable of entities to upload
        """
        chunk = []
        for entity in entities:
            if hasattr(entity, 'to_dict'):
                entity = entity.to_dict()
            chunk.append(entity)
            if len(chunk) >= chunk_size:
                self._bulk_chunk(collection_id, chunk, **kw)
                chunk = []
        if len(chunk):
            self._bulk_chunk(collection_id, chunk, **kw)

    def match(self, entity: Dict, collection_ids: Optional[str] = None,
              url: str = None, publisher: bool = False) -> Iterator[List]:
        """Find similar entities given a sample entity."""
        params = {'collection_ids': ensure_list(collection_ids)}
        if url is None:
            url = self._make_url('match')
        try:
            response = self.session.post(url, json=entity, params=params)
            response.raise_for_status()
            for result in response.json().get('results', []):
                yield self._patch_entity(result, publisher=publisher)
        except RequestException as exc:
            raise AlephException(exc)

    def linkages(self, context_ids: Optional[List] = None,
                 publisher: bool = False) -> 'APIResultSet':
        """Stream all linkages within the given role contexts."""
        filters = [('context_id', c) for c in ensure_list(context_ids)]
        url = self._make_url('linkages', filters=filters)
        return LinkageResultSet(self, url, publisher=publisher)

    def ingest_upload(self, collection_id: str,
                      file_path: Optional[Path] = None,
                      metadata: Optional[Dict] = None,
                      sync: bool = False,
                      index: bool = True) -> Dict:
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
        url_path = "collections/{0}/ingest".format(collection_id)
        url = self._make_url(url_path, sync=sync, index=index)
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
