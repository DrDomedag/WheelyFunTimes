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
import util
import prerequestfiles


def get_weather_forecast():
    city = "Malmö"
    latitude = 55.3535
    longitude = 13.0117

    weather_data_forecast_df = weather_data.get_hourly_weather_forecast(city, latitude, longitude)

    weather_data_forecast_df = weather_data_forecast_df.rename(columns={"date":"datetime"})

    weather_fg = fs.get_feature_group(
    name='weather',
    version=1,
    )

    weather_fg.insert(weather_data_forecast_df)


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
        columns={"röd dag": "holiday", "klämdag": "squeeze_day", "dag före arbetsfri helgdag": "day_before_holiday", "datum":"datetime"})

    date_df["datetime"] = pd.to_datetime(date_df['datetime'])

    date_df["arbetsfri dag"] = date_df["arbetsfri dag"].astype(bool)
    date_df["holiday"] = date_df["holiday"].astype(bool)
    date_df["helgdag"] = date_df["helgdag"].astype(bool)
    date_df["squeeze_day"] = date_df["squeeze_day"].astype(bool)
    date_df["helgdagsafton"] = date_df["helgdagsafton"].astype(bool)
    date_df["day_before_holiday"] = date_df["day_before_holiday"].astype(bool)
    date_df.info()
    #print(date_df.head(45))

    # Retrieve feature groups
    date_fg = fs.get_feature_group(
        name='date',
        version=1,
    )
    date_fg.insert(date_df)

def get_vehicle(): #date: str, company: str, outfolder: (str, None) = None
    date = datetime.now()
    date = date + timedelta(days=1)
    year = date.year
    month = date.month
    day = date.day

    date_string = f"{year}-{month}-{day}"
    company = "skane"
    data = get_static_custom.load_static_data(company, date_string, remove_unused_stations=True)
    stop_df = data.stop_times
    stop_df.info()
    stop_df = stop_df.drop(["arrival_time", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled", "location_type", "parent_station", "platform_code"], axis=1)
    stop_df.info()
    stop_df = stop_df.sort_values(["trip_id", "departure_time"])
    print(stop_df.head(14))
    trip_df = data.trips
    trip_df.info()

    #För stop - pos
    stop_pos_df = stop_df[["stop_name", "stop_lat", "stop_lon"]]
    stop_pos_df = stop_pos_df.drop_duplicates()

    trip_df = trip_df.drop(["trip_headsign", "service_id", "shape_id", "agency_id", "route_type", "route_desc"], axis=1)
    print("trips")
    trip_df.info()
    trip_df['route_id'] = trip_df.index.get_level_values('route_id')
    trip_df.info()

    merged_df = trip_df.merge(stop_df, how="left", on="trip_id")
    merged_df.reset_index()
    merged_df["trip_id"] = merged_df.index
    merged_df = merged_df.drop(["stop_name", "timepoint"], axis=1)
    #merged_df['datetime'] = pd.to_datetime(merged_df['timestamp'])
    
    merged_df = merged_df.rename(columns={"stop_lat":"vehicle_position_latitude", "stop_lon":"vehicle_position_longitude", "departure_time":"datetime"})

    merged_df.info()

    merged_df["direction_id"] = merged_df["direction_id"].astype(bool)

    # TODO - Töm/radera feature groupen innan så att vi inte har en massa gammal 'future'-data kvar.

    util.delete_feature_groups(fs, "vehicle_future")

    # Retrieve feature groups
    vehicle_fg = fs.get_or_create_feature_group(
        name='vehicle_future',
        description='Future data on vehicle routes',
        version=1,
        primary_key=['trip_id', "datetime"],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )
    vehicle_fg.insert(merged_df)

    #För stop - pos
    """stop_fg = fs.get_or_create_feature_group(
        name='stops',
        description='Positions for all stops',
        version=1,
        primary_key=['stop_lat', "stop_lon"],
        # expectation_suite=weather_expectation_suite
    )

    stop_fg.insert(stop_pos_df)"""

def update_historical_weather(date):
    # Get air quality feature group

    city = "Malmö"
    latitude = 55.3535
    longitude = 13.0117
    
    weather_df = weather_data.get_historical_weather(city, date, date, latitude, longitude)

    weather_df = weather_df.rename(columns={"date":"datetime"})

    weather_fg = fs.get_feature_group(
        name='weather',
        version=1,
    )
    
    weather_fg.insert(weather_df)
    

def update_historical_vehicle(yesterday_string, date):
    #Titta på dagens datum
    #Plocka data från koda från igår
    company = "skane"
    
    prerequestfiles.make_requests([date])

    vehicle_df = vehicle_data.get_vehicle_position_data(company, yesterday_string, 0, 23)

    vehicle_df["direction_id"] = vehicle_df["direction_id"].astype(bool)

    vehicle_fg = fs.get_feature_group(
    name='vehicle',
    version=1,
    )
    vehicle_fg.insert(vehicle_df) 

def get_weather(date):
    city = "Malmö"
    latitude = 55.3535
    longitude = 13.0117
    
    historical_weather_df = weather_data.get_historical_weather(city, date, date, latitude, longitude)

    weather_data_forecast_df = weather_data.get_hourly_weather_forecast(city, latitude, longitude)

    weather_df = pd.concat([historical_weather_df, weather_data_forecast_df])

    weather_df = weather_df.rename(columns={"date":"datetime"})

    weather_fg = fs.get_feature_group(
        name='weather',
        version=1,
    )
    
    weather_fg.insert(weather_df)


project = hopsworks.login(project="id2223AirQuality")
fs = project.get_feature_store()

#get_weather_forecast
#get_dates()
def update_historical(previous):
    now = datetime.now()
    yesterday = now - timedelta(days = previous)
    day = yesterday.day
    month = yesterday.month
    year = yesterday.year

    yesterday_string = yesterday.strftime("%Y-%m-%d")
    get_weather(yesterday_string)
    update_historical_vehicle(yesterday_string, yesterday)
    

def get_future():
    
    get_dates()
    get_vehicle()

"""update_historical()
get_future()"""


    
    
