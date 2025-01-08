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


CACHE_DIR = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))

os.makedirs(CACHE_DIR, exist_ok=True)


N_CPU = str(config_data.get('n_cpu', -1))
KODA_API_KEY = config_data.get('koda_api_key', '')
GTFS_STATIC_KEY = config_data.get('gtfs_static_key', '')

os.environ["N_CPU"] = N_CPU
os.environ["KODA_API_KEY"] = KODA_API_KEY
os.environ["GTFS_STATIC_KEY"] = GTFS_STATIC_KEY


import inference
import visualisation
import util
import backfill
import feature_selection


project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()




"""Project purge"""
# UNCOMMENT TO REMOVE **EVERYTHING**
#util.purge_project(project)
#util.purge_project(project)


"""Data request pipeline"""
#import prerequestfiles

"""dates = []
for i in range(31):
    date = datetime(year=2024, month=12, day=(i+1))
    dates.append(date)"""

#prerequestfiles.make_requests(dates)


"""Backfill pipeline"""
'''
dates = []
year = 2024
month = 12
for i in range(31):
    dates.append(datetime(year=year, month=month, day=(i+1)))
backfill.backfill_list(fs, dates)
'''

"""Feature pipeline"""
#import feature_update
#feature_update.get_future(fs)
#feature_update.update_historical(fs, 2)

"""Training pipeline"""
#import training
#train_from_local_data = False
#training.train(fs, mr, show_plot=False, train_from_local_data=train_from_local_data, upload_model=True, do_random_hyperparameter_search=False)

"""Inference pipeline"""
#result_df = inference.inference(fs, mr)

"""Visualisation pipeline"""
#visualisation.visualise(fs, result_df)


"""Group positions by closest station"""
#import group_position
# NOTE: We tried to make this work, but it too far too long to be feasible. It may be a path for further exploration with some significant optimisation and more computational power.
#group_position.merge_stop(fs)
#group_position.test(fs)

"""Feature importance metrics"""
#import feature_selection
#feature_selection.get_features(mr)

"""Correlation matrix"""
#import feature_selection
# Assumes local data available, which can be downloaded with the code in save_data_locally.py
#df = pd.read_csv(os.environ["cache_dir"] + '/training_data.csv')
#feature_selection.correlation_matrix(df)

