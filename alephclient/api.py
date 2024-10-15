import importlib.metadata
import json
import uuid
import logging
from itertools import count
from pathlib import Path
from urllib.parse import urlencode, urljoin
from banal import ensure_dict, ensure_list
from requests import RequestException, Session
from requests.exceptions import HTTPError
from requests_toolbelt import MultipartEncoder  # type: ignore
from typing import Dict, Mapping, Iterable, Iterator, List, Optional, Any

from alephclient import settings
from alephclient.errors import AlephException
from alephclient.util import backoff, prop_push

log = logging.getLogger(__name__)
MIME = "application/octet-stream"
VERSION = importlib.metadata.version("alephclient")


class APIResultSet(object):
    def __init__(self, api: "AlephAPI", url: str):
        self.api = api
        self.url = url
        self.current = 0
        self.result = self.api._request("GET", self.url)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= self.result.get("limit"):
            next_url = self.result.get("next")
            if next_url is None:
                raise StopIteration
            self.result = self.api._request("GET", next_url)
        try:
            item = self.result.get("results", [])[self.index]
        except IndexError:
            raise StopIteration
        self.current += 1
        return self._patch(item)

    next = __next__

    def _patch(self, item):
        return item

    @property
    def index(self):
        return self.current - self.result.get("offset")

    def __len__(self):
        return self.result.get("total")

    def __repr__(self):
        return "<APIResultSet(%r, %r)>" % (self.url, len(self))


class EntityResultSet(APIResultSet):
    def __init__(self, api: "AlephAPI", url: str, publisher: bool):
        super(EntityResultSet, self).__init__(api, url)
        self.publisher = publisher

    def _patch(self, item):
        return self.api._patch_entity(item, self.publisher)


class EntitySetItemsResultSet(APIResultSet):
    def __init__(self, api: "AlephAPI", url: str, publisher: bool):
        super(EntitySetItemsResultSet, self).__init__(api, url)
        self.publisher = publisher

    def _patch(self, item):
        entity = ensure_dict(item.get("entity"))
        item["entity"] = self.api._patch_entity(entity, self.publisher)
        return item


class AlephAPI(object):
    def __init__(
        self,
        host: Optional[str] = settings.HOST,
        api_key: Optional[str] = settings.API_KEY,
        session_id: Optional[str] = None,
        retries: int = settings.MAX_TRIES,
    ):
        if not host:
            raise AlephException("No host environment variable found")
        self.base_url = urljoin(host, "/api/2/")
        self.retries = retries
        session_id = session_id or str(uuid.uuid4())
        self.session: Session = Session()
        self.session.headers["X-Aleph-Session"] = session_id
        self.session.headers["User-Agent"] = "alephclient/%s" % VERSION
        if api_key is not None:
            self.session.headers["Authorization"] = "ApiKey %s" % api_key

    def _make_url(
        self,
        path: str,
        query: Optional[str] = None,
        filters: Optional[List] = None,
        params: Optional[Mapping[str, Any]] = None,
    ):
        """Construct the target url from given args"""
        url = self.base_url + path
        params = params or {}
        params_list = list(params.items())
        if query:
            params_list.append(("q", query))
        if filters:
            for key, val in filters:
                if val is not None:
                    params_list.append(("filter:" + key, val))
        if len(params_list):
            params_filter = [(k, v) for k, v in params_list if v is not None]
            url = url + "?" + urlencode(params_filter)
        return url

    def _patch_entity(
        self, entity: Dict, publisher: bool, collection: Optional[Dict] = None
    ):
        """Add extra properties from context to the given entity."""
        properties: Dict = entity.get("properties", {})
        collection_: Dict = collection or entity.get("collection") or {}
        links: Dict = entity.get("links", {})
        api_url = links.get("self")
        if api_url is None:
            api_url = "entities/%s" % entity.get("id")
            api_url = self._make_url(api_url)
        prop_push(properties, "alephUrl", api_url)

        if publisher:
            # Context: setting the original publisher or collection
            # label can help make the data more traceable when merging
            # data from multiple sources.
            publisher_label = collection_.get("label")
            publisher_label = collection_.get("publisher", publisher_label)
            prop_push(properties, "publisher", publisher_label)

            publisher_url = collection_.get("links", {}).get("ui")
            publisher_url = collection_.get("publisher_url", publisher_url)
            prop_push(properties, "publisherUrl", publisher_url)

        entity["properties"] = properties
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
        except (RequestException, HTTPError) as exc:
            raise AlephException(exc) from exc

        if len(response.text):
            return response.json()
        return {}

    def search(
        self,
        query: str,
        schema: Optional[str] = None,
        schemata: Optional[str] = None,
        filters: Optional[List] = None,
        publisher: bool = False,
        params: Optional[Mapping[str, Any]] = None,
    ) -> "EntityResultSet":
        """Conduct a search and return the search results."""
        filters_list: List = ensure_list(filters)
        if schema is not None:
            filters_list.append(("schema", schema))
        if schemata is not None:
            filters_list.append(("schemata", schemata))
        if schema is None and schemata is None:
            filters_list.append(("schemata", "Thing"))
        url = self._make_url(
            "entities", query=query, filters=filters_list, params=params
        )
        return EntityResultSet(self, url, publisher)

    def get_collection(self, collection_id: str) -> Dict:
        """Get a single collection by ID (not foreign ID!)."""
        url = self._make_url(f"collections/{collection_id}")
        return self._request("GET", url)

    def reingest_collection(self, collection_id: str, index: bool = False):
        """Re-ingest all documents in a collection."""
        url = self._make_url(
            f"collections/{collection_id}/reingest", params={"index": index}
        )
        return self._request("POST", url)

    def reindex_collection(
        self, collection_id: str, flush: bool = False, sync: bool = False
    ):
        """Re-index all entities in a collection."""
        params = {"sync": sync, "flush": flush}
        url = self._make_url(f"collections/{collection_id}/reindex", params=params)
        return self._request("POST", url)

    def delete_collection(self, collection_id: str, sync: bool = False):
        """Delete a collection by ID"""
        params = {"sync": sync}
        url = self._make_url(f"collections/{collection_id}", params=params)
        return self._request("DELETE", url)

    def flush_collection(self, collection_id: str, sync: bool = False):
        """Empty all contents from a collection by ID"""
        params = {"sync": sync, "keep_metadata": True}
        url = self._make_url(f"collections/{collection_id}", params=params)
        return self._request("DELETE", url)

    def touch_collection(self, collection_id: str):
        """Update the content update date of a collection by ID"""
        url = self._make_url(f"collections/{collection_id}/touch")
        return self._request("POST", url)

    def get_entity(self, entity_id: str, publisher: bool = False) -> Dict:
        """Get a single entity by ID."""
        url = self._make_url(f"entities/{entity_id}")
        entity = self._request("GET", url)
        return self._patch_entity(entity, publisher)

    def delete_entity(self, entity_id: str) -> Dict:
        """Delete a single entity by ID."""
        url = self._make_url(f"entities/{entity_id}")
        return self._request("DELETE", url)

    def get_collection_by_foreign_id(self, foreign_id: str) -> Optional[Dict]:
        """Get a dict representing a collection based on its foreign ID."""
        if foreign_id is None:
            return None
        filters = [("foreign_id", foreign_id)]
        for coll in self.filter_collections(filters=filters):
            return coll
        return None

    def load_collection_by_foreign_id(
        self, foreign_id: str, config: Optional[Dict] = None
    ) -> Dict:
        """Get a collection by its foreign ID, or create one. Setting clear
        will clear any found collection."""
        collection = self.get_collection_by_foreign_id(foreign_id)
        if collection is not None:
            return collection

        config_: Dict = ensure_dict(config)
        return self.create_collection(
            {
                "foreign_id": foreign_id,
                "label": config_.get("label", foreign_id),
                "casefile": config_.get("casefile", False),
                "category": config_.get("category", "other"),
                "languages": config_.get("languages", []),
                "summary": config_.get("summary", ""),
            }
        )

    def filter_collections(
        self, query: Optional[str] = None, filters: Optional[List] = None, **kwargs
    ) -> "APIResultSet":
        """Filter collections for the given query and/or filters.

        params
        ------
        query: query string
        filters: list of key, value pairs to filter collections
        kwargs: extra arguments for api call such as page, limit etc
        """
        if not query and not filters:
            raise ValueError("One of query or filters is required")

        url = self._make_url("collections", query=query, filters=filters, params=kwargs)
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

    def update_collection(
        self, collection_id: str, data: Dict, sync: bool = False
    ) -> Dict:
        """Update an existing collection using the given data.

        params
        ------
        collection_id: id of the collection to update
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        params = {"sync": sync}
        url = self._make_url(f"collections/{collection_id}", params=params)
        return self._request("PUT", url, json=data)

    def stream_entities(
        self,
        collection: Optional[Dict] = None,
        include: Optional[List] = None,
        schema: Optional[str] = None,
        publisher: bool = False,
    ) -> Iterator[Dict]:
        """Iterate over all entities in the given collection.

        params
        ------
        collection_id: id of the collection to stream
        include: an array of fields from the index to include.
        """
        url = self._make_url("entities/_stream")
        if collection is not None:
            collection_id = collection.get("id")
            url = f"collections/{collection_id}/_stream"
            url = self._make_url(url)
        params = {"include": include, "schema": schema}
        try:
            res = self.session.get(url, params=params, stream=True)
            res.raise_for_status()
            for entity in res.iter_lines(chunk_size=None):
                entity = json.loads(entity)
                yield self._patch_entity(
                    entity, publisher=publisher, collection=collection
                )
        except (RequestException, HTTPError) as exc:
            raise AlephException(exc) from exc

    def _bulk_chunk(
        self,
        collection_id: str,
        chunk: List,
        entityset_id: Optional[str] = None,
        force: bool = False,
        unsafe: bool = False,
        cleaned: bool = False,
    ):
        for attempt in count(1):
            url = self._make_url(f"collections/{collection_id}/_bulk")
            params = {"entityset_id": entityset_id}
            if unsafe:
                params["safe"] = "false"
            if cleaned:
                params["clean"] = "false"
            try:
                response = self.session.post(url, json=chunk, params=params)
                response.raise_for_status()
                return
            except (RequestException, HTTPError) as exc:
                ae = AlephException(exc)
                if not ae.transient or attempt > self.retries:
                    if not force:
                        raise ae from exc
                    log.error(ae)
                    return
                backoff(ae, attempt)

    def write_entity(
        self, collection_id: str, entity: Dict, entity_id: Optional[str] = None, **kw
    ) -> Dict:
        """Create a single entity via the API, in the given
        collection.

        params
        ------
        collection_id: id of the collection to use. This will overwrite any
        existing collection specified in the entity dict
        entity_id: id for the entity to be created. This will overwrite any
        existing entity specified in the entity dict
        entity: A dict object containing the values of the entity
        """
        entity["collection_id"] = collection_id

        if entity_id is not None:
            entity["id"] = entity_id

        for attempt in count(1):
            if entity_id is not None:
                url = self._make_url("entities/{}").format(entity_id)
            else:
                url = self._make_url("entities")
            try:
                return self._request("POST", url, json=entity)
            except RequestException as exc:
                ae = AlephException(exc)
                if not ae.transient or attempt > self.retries:
                    log.error(ae)
                    raise exc
                backoff(ae, attempt)

        return {}

    def write_entities(
        self, collection_id: str, entities: Iterable, chunk_size: int = 1000, **kw
    ):
        """Create entities in bulk via the API, in the given
        collection.

        params
        ------
        collection_id: id of the collection to use
        entities: an iterable of entities to upload
        """
        chunk = []
        for entity in entities:
            if hasattr(entity, "to_dict"):
                entity = entity.to_dict()
            chunk.append(entity)
            if len(chunk) >= chunk_size:
                self._bulk_chunk(collection_id, chunk, **kw)
                chunk = []
        if len(chunk):
            self._bulk_chunk(collection_id, chunk, **kw)

    def match(
        self,
        entity: Dict,
        collection_ids: Optional[str] = None,
        url: Optional[str] = None,
        publisher: bool = False,
    ) -> Iterator[List]:
        """Find similar entities given a sample entity."""
        params = {"collection_ids": ensure_list(collection_ids)}
        if url is None:
            url = self._make_url("match")
        try:
            response = self.session.post(url, json=entity, params=params)  # type: ignore
            response.raise_for_status()
            for result in response.json().get("results", []):
                yield self._patch_entity(result, publisher=publisher)
        except (RequestException, HTTPError) as exc:
            raise AlephException(exc) from exc

    def entitysets(
        self,
        collection_id: Optional[str] = None,
        set_types: Optional[List] = None,
        prefix: Optional[str] = None,
    ) -> "APIResultSet":
        """Stream EntitySets"""
        filters_collection = [("collection_id", collection_id)]
        filters_type = [("type", t) for t in ensure_list(set_types)]
        filters = [*filters_collection, *filters_type]
        params = {"prefix": prefix}
        url = self._make_url("entitysets", filters=filters, params=params)
        return APIResultSet(self, url)

    def entitysetitems(
        self, entityset_id: str, publisher: bool = False
    ) -> "APIResultSet":
        url = self._make_url(f"entitysets/{entityset_id}/items")
        return EntitySetItemsResultSet(self, url, publisher=publisher)

    def ingest_upload(
        self,
        collection_id: str,
        file_path: Optional[Path] = None,
        metadata: Optional[Dict] = None,
        sync: bool = False,
        index: bool = True,
    ) -> Dict:
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
        params = {"sync": sync, "index": index}
        url = self._make_url(url_path, params=params)
        if not file_path or file_path.is_dir():
            data = {"meta": json.dumps(metadata)}
            return self._request("POST", url, data=data)

        for attempt in count(1):
            try:
                with file_path.open("rb") as fh:
                    # use multipart encoder to allow uploading very large files
                    m = MultipartEncoder(
                        fields={
                            "meta": json.dumps(metadata),
                            "file": (file_path.name, fh, MIME),
                        }
                    )
                    headers = {"Content-Type": m.content_type}
                    return self._request("POST", url, data=m, headers=headers)
            except AlephException as ae:
                if not ae.transient or attempt > self.retries:
                    raise ae from ae
                backoff(ae, attempt)
        return {}

    def create_entityset(
        self, collection_id: str, type: str, label: str, summary: Optional[str]
    ) -> Dict:
        """Create an EntitySet inside a collection"""
        url = self._make_url("entitysets")
        data: Dict = {
            "collection_id": collection_id,
            "type": type,
            "label": label,
            "summary": summary,
            "entities": [],
        }
        return self._request("POST", url, data=data)

    def delete_entityset(self, entityset_id: str, sync: bool = False):
        """Delete an EntitySet by id"""
        url = self._make_url(f"entitysets/{entityset_id}", params={"sync": sync})
        return self._request("DELETE", url)
