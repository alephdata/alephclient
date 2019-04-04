import os

# Aleph client API settings
ALEPH_HOST = os.environ.get('MEMORIOUS_ALEPH_HOST')
ALEPH_HOST = os.environ.get('ALEPH_HOST', ALEPH_HOST)

ALEPH_API_KEY = os.environ.get('MEMORIOUS_ALEPH_API_KEY')
ALEPH_API_KEY = os.environ.get('ALEPH_API_KEY', ALEPH_API_KEY)
