import os
import multiprocessing

# Aleph client API settings
HOST = os.environ.get('MEMORIOUS_ALEPH_HOST')
HOST = os.environ.get('ALEPH_HOST', HOST)

API_KEY = os.environ.get('MEMORIOUS_ALEPH_API_KEY')
API_KEY = os.environ.get('ALEPH_API_KEY', API_KEY)

THREADS = int(os.getenv("ALEPHCLIENT_THREADS", 5 * multiprocessing.cpu_count()))  # noqa
TIMEOUT = int(os.getenv("ALEPHCLIENT_TIMEOUT", 5))
MAX_TRIES = int(os.getenv("ALEPHCLIENT_MAX_TRIES", 3))
