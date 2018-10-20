import uuid
import json
from six.moves.urllib.parse import urlencode, urljoin
import requests
from requests_toolbelt import MultipartEncoder

from alephclient.errors import AlephException


class AlephAPI(object):

    def __init__(self, base_url, api_key, session_id=None):
        self.base_url = urljoin(base_url, '/api/2/')
        self.session = requests.Session()
        self.session.headers = {
            'X-Aleph-Session': session_id or str(uuid.uuid4()),
            'Authorization': 'ApiKey %s' % api_key
        }

    def _make_url(self, path, query=None, filters=None, **kwargs):
        """Construct the target url from given args"""
        params = kwargs
        if query:
            params["q"] = query
        if filters:
            for key, val in filters:
                params["filter:"+key] = val
        return self.base_url + path + '?' + urlencode(params)

    def _request(self, method, url, **kwargs):
        """A single point to make the http requests.

        Having a single point to make all requests let's us set headers, manage
        successful and failed responses and possibly manage session etc
        conviniently in a single place.
        """
        response = self.session.request(method=method, url=url, **kwargs)
        if response.status_code > 299:
            raise AlephException(response)
        response.raise_for_status()
        if len(response.text):
            return response.json()

    def get_collection(self, collection_id):
        url = self._make_url("collections/{0}".format(collection_id))
        return self._request("GET", url)

    def get_collection_by_foreign_id(self, foreign_id):
        filters = [('foreign_id', foreign_id)]
        for coll in self.filter_collections(filters=filters, limit=1):
            if coll.get('foreign_id') == foreign_id:
                return coll

    def filter_collections(self, query=None, filters=None, **kwargs):
        """Filter collections for the given query and/or filters.

        params
        ------
        query: query string
        filters: list of key, value pairs to filter collections
        kwargs: extra arguments for api call such as page, limit etc
        """
        if not query and not filters:
            raise ValueError("One of query or filters is required")
        url = self._make_url("collections", filters=filters, **kwargs)
        res = self._request("GET", url)
        if res is not None:
            return res.get("results", [])

    def create_collection(self, data):
        """Create a collection from the given data.

        params
        ------
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        url = self._make_url("collections")
        return self._request("POST", url, json=data)

    def update_collection(self, collection_id, data):
        """Update an existing collection using the given data.

        params
        ------
        collection_id: id of the collection to update
        data: dict with foreign_id, label, category etc. See `CollectionSchema`
        for more details.
        """
        url = self._make_url("collections/{0}".format(collection_id))
        return self._request("PUT", url, json=data)

    def map_collection(self, collection_id, mapping):
        """Run a bulk entity data mapping on a collection.

        params
        ------
        collection_id: id of the collection to update
        mapping: dict with the entity bulk load mapping.
        """
        url = self._make_url("collections/{0}/mapping".format(collection_id))
        return self._request("PUT", url, json=mapping)

    def stream_entities(self, collection_id=None, include=None):
        url = urljoin(self.base_url, 'entities/_stream')
        if collection_id is not None:
            url = 'collections/%s/_stream' % collection_id
            url = urljoin(self.base_url, url)
        params = {'include': include}
        res = self.session.get(url, params=params, stream=True)
        for line in res.iter_lines():
            yield json.loads(line)

    def ingest_upload(self, collection_id, file_path=None, metadata=None):
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

        with file_path.open('rb') as fh:
            # use multipart encoder to allow uploading very large files
            m = MultipartEncoder(fields={
                'meta': json.dumps(metadata),
                'file': (file_path.name, fh, 'application/octet-stream')
            })
            headers = {'Content-Type': m.content_type}
            return self._request("POST", url, data=m, headers=headers)
