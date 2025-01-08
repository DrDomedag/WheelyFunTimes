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


import inference
import visualisation
import util
import backfill
import feature_selection


project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()
mr = project.get_model_registry()



'''
date = datetime.now()
date = date - timedelta(days=8)

date = datetime(year=2024, month=12, day=15)

year = date.year
month = date.month
day = date.day
train_test_data_split_time = f"{year}-{month}-{day}"

result_df = None
'''

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
'''

'''
#dates.append(datetime(year=year, month=month, day=29)) # Testar utan 29 - failar på static.
year = 2024
month = 12
dates.append(datetime(year=year, month=month, day=30))
dates.append(datetime(year=year, month=month, day=31))

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

backfill.backfill_list(fs, dates)
'''

"""Feature pipeline"""
#import feature_update
#feature_update.get_future(fs)
#feature_update.update_historical(fs, 3)

"""Training pipeline"""
import training
train_from_local_data = True
training.train(fs, mr, show_plot=False, train_from_local_data=train_from_local_data, upload_model=True, do_random_hyperparameter_search=False)

"""Inference pipeline"""
#result_df = inference.inference(fs, mr)

"""Visualisation pipeline"""
#visualisation.visualise(fs, result_df)

"""previous = 2

now = datetime.now()
yesterday = now - timedelta(days = previous)
yesterday_string = yesterday.strftime("%Y-%m-%d")

feature_update.get_weather(yesterday_string)"""
#import group_position
#group_position.merge_stop(fs)

#Get the most imprtance features
#feature_selection.get_features(mr)

# Correlation matrix
'''
df = pd.read_csv(os.environ["cache_dir"] + '/training_data.csv')

df = df.dropna()
#df.info()
df = df.drop(['route_long_name'], axis=1)

availability_mapping = {
        'EMPTY': 0,
        'MANY_SEATS_AVAILABLE': 1,
        'FEW_SEATS_AVAILABLE': 2,
        'STANDING_ROOM_ONLY': 3,
        'CRUSHED_STANDING_ROOM_ONLY': 4,
        'FULL': 5
    }

df['vehicle_occupancy_status'] = df['vehicle_occupancy_status'].map(availability_mapping)

df['datetime'] = pd.to_datetime(df['datetime'])
df['dag_i_vecka'] = df['dag_i_vecka'].astype('category')
#df['route_long_name'] = df['route_long_name'].astype('category')
#training_data['route_id'] = training_data['route_id'].astype('category')

df['vehicle_occupancy_status'] = df['vehicle_occupancy_status'].astype('int')
df["arbetsfri_dag"] = df["arbetsfri_dag"].astype("bool")
df["holiday"] = df["holiday"].astype("bool")
df["helgdag"] = df["helgdag"].astype("bool")
df["squeeze_day"] = df["squeeze_day"].astype("bool")
df["helgdagsafton"] = df["helgdagsafton"].astype("bool")
df["day_before_holiday"] = df["day_before_holiday"].astype("bool")

feature_selection.correlation_matrix(df)
'''
