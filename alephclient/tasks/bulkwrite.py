import json
import logging

from alephclient.tasks.util import load_collection

log = logging.getLogger(__name__)


def read_json_stream(stream):
    count = 0
    try:
        while True:
            line = stream.readline()
            if not line:
                return
            count += 1
            if count % 1000 == 0:
                log.info("Bulk load entities: %s...", count)
            yield json.loads(line)
    except Exception:
        pass


def bulk_write(api, stream, foreign_id):
    collection_id = load_collection(api, foreign_id, {})
    entities = read_json_stream(stream)
    api.bulk_write(collection_id, entities)
