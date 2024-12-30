import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import appdirs
import configparser
import pandas as pd
import hopsworks
from datetime import datetime, timedelta

import training
import feature_update
import inference
import visualisation
import util
import backfill


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

N_CPU = str(config_data.get('n_cpu', -1))
KODA_API_KEY = config_data.get('koda_api_key', '')
GTFS_STATIC_KEY = config_data.get('gtfs_static_key', '')

os.environ["N_CPU"] = N_CPU
os.environ["KODA_API_KEY"] = KODA_API_KEY
os.environ["GTFS_STATIC_KEY"] = GTFS_STATIC_KEY

#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))


project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()




date = datetime.now()
date = date - timedelta(days=8)

date = datetime(year=2024, month=12, day=15)

year = date.year
month = date.month
day = date.day
train_test_data_split_time = f"{year}-{month}-{day}"

result_df = None

"""Project purge"""
# UNCOMMENT TO REMOVE **EVERYTHING**
util.purge_project(project)

"""Backfill pipeline"""
backfill.backfill(fs, start_date=date, days=3)

"""Feature pipeline"""
#feature_update.get_future()
#feature_update.update_historical(2)

"""Training pipeline"""
#training.train(fs, mr, train_test_data_split_time, plot=False)

"""Inference pipeline"""
#result_df = inference.inference(fs, mr)

"""Visualisation pipeline"""
#visualisation.visualise(fs, result_df)

