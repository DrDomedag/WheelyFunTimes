import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

os.environ["cache_dir"] = os.path.join(os.environ["RUNNER_TEMP"], './cache_dir')
N_CPU = str(os.cpu_count())

import appdirs
import configparser
import pandas as pd
import hopsworks
from datetime import datetime, timedelta

import feature_update


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)





"""default_config = {
    'api_key': '',
    'cache_dir': appdirs.user_cache_dir('pykoda'),
    'n_cpu': str(os.cpu_count())
}"""


"""CONFIG_DIR = appdirs.user_config_dir('pykoda')
CONFIG_FILE = CONFIG_DIR + "\\config.ini"
if os.path.exists(CONFIG_DIR):
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)

    config_data = parser['all']
else:
    config_data = dict()"""

#CACHE_DIR = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))

#os.makedirs(CACHE_DIR, exist_ok=True)

#API_KEY = config_data.get('api_key', '')

#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))
print(f'Found hopsworks_api_key: {"HOPSWORKS_API_KEY" in os.environ}')

project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()


"""Feature pipeline"""
days_prior=3

feature_update.get_future(fs)
feature_update.update_historical(fs, days_prior)

