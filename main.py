import os
import appdirs

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import configparser
import pandas as pd
import hopsworks
from datetime import datetime, timedelta




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

#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))


import training
import inference
import visualisation
import util
import backfill


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
#util.purge_project(project)
#util.purge_project(project)


"""Data request pipeline"""
import prerequestfiles

dates = []
for i in range(14):
    date = datetime(year=2024, month=12, day=(i+1))
    date = datetime.strftime(date, "%Y-%m-%d")
    dates.append(date)

#prerequestfiles.make_requests(dates)


"""Backfill pipeline"""
#backfill.backfill(fs, start_date=date, days=3)
"""Backfill pipeline"""
#backfill.backfill(fs)
#day_list = [2, 3, 7, 9, 10, 12]
dates = []
#dates.append(datetime(year=year, month=month, day=15))

'''
dates.append(datetime(year=year, month=month, day=15))
dates.append(datetime(year=year, month=month, day=16))
dates.append(datetime(year=year, month=month, day=17))
dates.append(datetime(year=year, month=month, day=19))
dates.append(datetime(year=year, month=month, day=22))
dates.append(datetime(year=year, month=month, day=24))
dates.append(datetime(year=year, month=month, day=25))
dates.append(datetime(year=year, month=month, day=26))

dates.append(datetime(year=year, month=month, day=14))
dates.append(datetime(year=year, month=month, day=13))
dates.append(datetime(year=year, month=month, day=12))
dates.append(datetime(year=year, month=month, day=11))
dates.append(datetime(year=year, month=month, day=10))
dates.append(datetime(year=year, month=month, day=9))
dates.append(datetime(year=year, month=month, day=8))
dates.append(datetime(year=year, month=month, day=7))
dates.append(datetime(year=year, month=month, day=6))
dates.append(datetime(year=year, month=month, day=5))
dates.append(datetime(year=year, month=month, day=4))
dates.append(datetime(year=year, month=month, day=3))
dates.append(datetime(year=year, month=month, day=2))
dates.append(datetime(year=year, month=month, day=1))
'''
dates.append(datetime(year=year, month=month, day=1))

#backfill.backfill_list(fs, dates)

"""Feature pipeline"""
#import feature_update
#feature_update.get_future()
#feature_update.update_historical(2)

"""Training pipeline"""
#training.train(fs, mr, train_test_data_split_time, plot=False)

"""Inference pipeline"""
#result_df = inference.inference(fs, mr)

"""Visualisation pipeline"""
#visualisation.visualise(fs, result_df)

