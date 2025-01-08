import os
import time

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import appdirs
import configparser
import pandas as pd
import hopsworks
from datetime import datetime, timedelta

import inference


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


default_config = {
    'api_key': '',
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
os.makedirs(CACHE_DIR, exist_ok=True)

N_CPU = int(config_data.get('n_cpu', -1))
API_KEY = config_data.get('api_key', '')

#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))

# This runs immediately following run_feature_update, so we wait for Hopsworks ingestion before attempting to get the data we just uploaded for inference purposes.
# This could presumably also be done in the github action, but this is good enough.
wait_time = 5 * 60
time.sleep(wait_time)

project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()

"""Inference pipeline"""
result_df = inference.inference(fs, mr)

"""Visualisation pipeline"""
#visualisation.visualise(fs, result_df)

