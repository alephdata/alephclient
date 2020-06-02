import time
import random
import logging
from typing import Dict
from banal import ensure_list

log = logging.getLogger(__name__)


def backoff(err, failures: int):
    """Implement a random, growing delay between external service retries."""
    sleep = (2 ** max(1, failures)) + random.random()
    log.warning("Error: %s, back-off: %.2fs", err, sleep)
    time.sleep(sleep)


def prop_push(properties: Dict, prop: str, value):
    values = ensure_list(properties.get(prop))
    values.extend(ensure_list(value))
    properties[prop] = values
