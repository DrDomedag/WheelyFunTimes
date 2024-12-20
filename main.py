import os

import weather_data

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

import appdirs
import configparser
import pandas as pd
import hopsworks
from date_data import get_calendar_data

import vehicle_data

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

city = "Malmö"
latitude = 55.3535
longitude = 13.0117
company = "skane"
year = "2022"
month = "08"
day = "21"
date = f"{year}-{month}-{day}"
hour = 12

start_hour = 15
end_hour = 15

#df = vehicle_data.get_vehicle_position_data(company, date, start_hour, end_hour)
#df.info()

#date_df = get_calendar_data(year, month, day)
#date_df.info()


#project = hopsworks.login(project="id2223AirQuality")

#fs = project.get_feature_store()
#mr = project.get_model_registry()

historical_weather_data_df = weather_data.get_historical_weather(city, date, date, latitude, longitude)

#historical_weather_data_df.info()
print(historical_weather_data_df.head())

weather_data_forecast_df = weather_data.get_hourly_weather_forecast(city, latitude, longitude)

#weather_data_forecast_df.info()
print(weather_data_forecast_df.head())

