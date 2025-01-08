import vehicle_data
import weather_data
from date_data import get_calendar_data
import pandas as pd
from datetime import datetime, timedelta
import pykoda_main.src.pykoda.data.datautils as datautils

city = "Malmö"
latitude = 55.3535
longitude = 13.0117
company = "skane"


def backfill_dates(fs, year, month, day):
    date_df = get_calendar_data(year, month, day)

    date_df = date_df.rename(columns={"röd dag": "holiday", "klämdag": "squeeze_day", "dag före arbetsfri helgdag": "day_before_holiday", "datum": "datetime"})

    date_df["datetime"] = pd.to_datetime(date_df['datetime'])
    date_df["dag i vecka"] = date_df["dag i vecka"].astype("category")
    date_df["arbetsfri dag"] = date_df["arbetsfri dag"].astype(bool)
    date_df["holiday"] = date_df["holiday"].astype(bool)
    date_df["helgdag"] = date_df["helgdag"].astype(bool)
    date_df["squeeze_day"] = date_df["squeeze_day"].astype(bool)
    date_df["helgdagsafton"] = date_df["helgdagsafton"].astype(bool)
    date_df["day_before_holiday"] = date_df["day_before_holiday"].astype(bool)


    date_fg = fs.get_or_create_feature_group(
        name='date',
        description='Information about Swedish holidays',
        version=1,
        primary_key=['datetime'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    date_fg.insert(date_df)

def backfill_weather(fs, date):
    historical_weather_data_df = weather_data.get_historical_weather(city, date, date, latitude, longitude)

    historical_weather_data_df = historical_weather_data_df.rename(columns={"date":"datetime"})

    weather_fg = fs.get_or_create_feature_group(
        name='weather',
        description='Weather characteristics of each hour',
        version=1,
        primary_key=['datetime'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    weather_fg.insert(historical_weather_data_df)



def backfill_vehicles(fs, date, start_hour, end_hour):
    vehicle_df, static_data = vehicle_data.get_vehicle_position_data(company, date, start_hour, end_hour)
    
    vehicle_df["direction_id"] = vehicle_df["direction_id"].astype(bool)

    vehicle_fg = fs.get_or_create_feature_group(
        name='vehicle',
        description='Data on vehicles',
        version=1,
        primary_key=['id'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    vehicle_fg.insert(vehicle_df)

def backfill_stop_times(fs, date):
    dfs = []

    for day in range(31):
        date = datetime(year=2024, month=12, day=(day+1))
        date = date.strftime("%Y-%m-%d")
        static_data = datautils.load_static_data(company="skane", date=date, remove_unused_stations=True)
        dfs.append(static_data.stop_times)

    stop_times_df = pd.concat(dfs, ignore_index=False)

    stop_times_df = stop_times_df.reset_index()

    stop_times_df = stop_times_df[["stop_name", "stop_lat", "stop_lon", "trip_id"]]

    stop_times_fg = fs.get_or_create_feature_group(
        name='stop_times',
        description='Data on stop times',
        version=1,
        primary_key=['trip_id', 'stop_id'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    stop_times_fg.insert(stop_times_df)


def backfill(fs, start_date, days):
    
    for i in range(days):

        date = start_date + timedelta(days=i)
        
        backfill_single_date(fs, date)


'''
def backfill_list(fs, days):
    now = datetime.now()
    for day in days:
        date = now - timedelta(day)
        backfill_single_date(fs, date)
'''

def backfill_list(fs, dates):
    for date in dates:
        backfill_single_date(fs, date)


def backfill_single_date(fs, date):
    
    year = date.year
    month = date.month
    day = date.day

    #date = f"{year}-{month}-{day}"
    date = datetime.strftime(date, "%Y-%m-%d")
    print(f"Running backfill for date: {date}")

    start_hour = 0
    end_hour = 23

    backfill_dates(fs, year, month, day)
    backfill_weather(fs, date)
    backfill_vehicles(fs, date, start_hour, end_hour)
    backfill_stop_times(fs, date)



