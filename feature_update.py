import os

os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"

from date_data import get_calendar_data
import weather_data
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pykoda_main.src.pykoda as pk



def get_weather_forecast(fs, city, latitude, longitude):
    weather_data_forecast_df = weather_data.get_hourly_weather_forecast(city, latitude, longitude)

    """ weather_fg = fs.get_or_create_feature_group(
        name='weather',
        description='Weather characteristics of each hour',
        version=1,
        primary_key=['date'],
        event_time="date",
        # expectation_suite=weather_expectation_suite
    )"""

    # weather_data_forecast_df.info()
    # print(weather_data_forecast_df.head())

    """weather_fg.insert(weather_data_forecast_df)"""
    # weather_data_forecast_df.info()
    # print(weather_data_forecast_df.head())


def get_dates():
    now = datetime.now()
    #month = nuvarande månad
    year = now.year
    month = now.month
    next_month = now + relativedelta(months=+1)
    next_year = next_month.year
    next_month = next_month.month
    
    #print(f"Year: {year}, month: {month}, next year: {next_year}, next month: {next_month}")

    date_df = get_calendar_data(year, month)
    date_df = pd.concat([date_df, get_calendar_data(next_year, next_month)], ignore_index=True)

    date_df = date_df.rename(
        columns={"röd dag": "holiday", "klämdag": "squeeze_day", "dag före arbetsfri helgdag": "day_before_holiday"})

    date_df["datum"] = pd.to_datetime(date_df['datum'])

    date_df.info()
    #print(date_df.head(45))

    """date_fg = fs.get_or_create_feature_group(
        name='date',
        description='Information about Swedish holidays',
        version=1,
        primary_key=['datum'],
        event_time="datum",
        # expectation_suite=weather_expectation_suite
    )

    date_fg.insert(date_df)"""

def get_vehicle(): #date: str, company: str, outfolder: (str, None) = None
    date = datetime.now()
    date = date - timedelta(days=1)
    year = date.year
    month = date.month
    day = date.day

    date_string = f"{year}-{month}-{day}"
    company = "skane"
    data = pk.datautils.load_static_data(company, date_string, remove_unused_stations=True)
    stop_df = data.stop_times
    stop_df.info()
    print(stop_df.head(14))

get_vehicle()
    
