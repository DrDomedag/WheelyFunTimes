#Start with XGBoost as baseline¨
#Then test LSTM

import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

from date_data import get_calendar_data
import weather_data
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pykoda_main.src.pykoda as pk
import get_static_custom
import hopsworks
import vehicle_data

with open('HOPSWORKS_API_KEY.txt', 'r') as file:
    os.environ["HOPSWORKS_API_KEY"] = file.read().rstrip()

project = hopsworks.login(project="id2223AirQuality")
fs = project.get_feature_store()

weather_fg = fs.get_feature_group(
    name='weather',
    version=1,
)

date_fg = fs.get_feature_group(
    name="date",
    version=1,
)

vehicle_fg = fs.get_feature_group(
    name="vehicle",
    version=1,
)

#Använd read för historisk data

