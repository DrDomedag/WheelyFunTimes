import os
import warnings
import configparser

import appdirs


default_config = {
    'api_key': '',
    'gtfs_static_key': '',
    'cache_dir': appdirs.user_cache_dir('pykoda'),
    'n_cpu': str(os.cpu_count())
}

CONFIG_DIR = appdirs.user_config_dir('pykoda')
CONFIG_FILE = CONFIG_DIR + "\\config.ini"
if os.path.exists(CONFIG_DIR):
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)

    config_data = parser['all']
else:
    config_data = dict()

CACHE_DIR = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))
#CACHE_DIR = os.environ["cache_dir"]
print(CACHE_DIR)
os.makedirs(CACHE_DIR, exist_ok=True)

N_CPU = int(config_data.get('n_cpu', -1))
API_KEY = config_data.get('api_key', '')
GTFS_STATIC_KEY = config_data.get('gtfs_static_key', '')
if not API_KEY:
    _msg = f'Config file {CONFIG_FILE} is missing the api key, please specify the parameter "api_key".' \
           'Falling back to v1 of the API for download.'
    warnings.warn(RuntimeWarning(_msg))
    API_VERSION = 1
else:
    API_VERSION = 2
