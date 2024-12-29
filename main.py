import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import appdirs
import configparser
import pandas as pd
import hopsworks
from datetime import datetime, timedelta

"""import util
import backfill"""
import training
import feature_update
import inference





pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# Bestäm var vi vill lagra key
with open('HOPSWORKS_API_KEY.txt', 'r') as file:
    os.environ["HOPSWORKS_API_KEY"] = file.read().rstrip()


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


project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()


# UNCOMMENT TO REMOVE **EVERYTHING**
#util.purge_project(project)

date = datetime.now()
date = date - timedelta(days=4)
year = date.year
month = date.month
day = date.day
train_test_data_split_time = f"{year}-{month}-{day}"

"""Backfill pipeline"""
#backfill.backfill(fs)

"""Feature pipeline"""
#feature_update.update_historical()
#feature_update.get_future()

"""Training pipeline"""
#training.train(fs, mr, train_test_data_split_time)

"""Inference pipeline"""
inference.inference(fs, mr)


