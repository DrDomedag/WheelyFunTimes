import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import hopsworks
import appdirs
import configparser
import pandas as pd

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

os.environ["cache_dir"] = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))

#os.makedirs(os.environ["cache_dir"], exist_ok=True)


CACHE_DIR = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))

os.makedirs(CACHE_DIR, exist_ok=True)


N_CPU = str(config_data.get('n_cpu', -1))
KODA_API_KEY = config_data.get('koda_api_key', '')
GTFS_STATIC_KEY = config_data.get('gtfs_static_key', '')

os.environ["N_CPU"] = N_CPU
os.environ["KODA_API_KEY"] = KODA_API_KEY
os.environ["GTFS_STATIC_KEY"] = GTFS_STATIC_KEY


'''
project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()

import training
df = training.get_training_data(fs)
filepath = CACHE_DIR + "/training_data.csv"
df.to_csv(filepath, sep=',', index=True, header=True, encoding=None)
'''

import pykoda_main.src.pykoda.data.datautils as datautils
from datetime import datetime

dfs = []

for day in range(31):
    date = datetime(year=2024, month=12, day=(day+1))
    date = date.strftime("%Y-%m-%d")
    static_data = datautils.load_static_data(company="skane", date=date, remove_unused_stations=True)
    dfs.append(static_data.stop_times)

stop_times_df = pd.concat(dfs, ignore_index=False)

stop_times_df = stop_times_df.reset_index()

stop_times_df = stop_times_df[["trip_id", "stop_name", "stop_lat", "stop_lon"]]

filepath = CACHE_DIR + "/stop_times.csv"
stop_times_df.to_csv(filepath, sep=',', index=True, header=True, encoding=None)
