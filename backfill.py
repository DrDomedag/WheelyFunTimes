import vehicle_data
import weather_data
from date_data import get_calendar_data
import pandas as pd
from datetime import datetime, timedelta

city = "Malmö"
latitude = 55.3535
longitude = 13.0117
company = "skane"


def backfill_dates(fs, year, month, day):
    date_df = get_calendar_data(year, month, day)

    date_df = date_df.rename(columns={"röd dag": "holiday", "klämdag": "squeeze_day", "dag före arbetsfri helgdag": "day_before_holiday", "datum": "datetime"})

    date_df["datetime"] = pd.to_datetime(date_df['datetime'])

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

    #historical_weather_data_df.info()
    #print(historical_weather_data_df.head(24))

    # Borde vara hyfsat lik, men ska uppdateras
    weather_fg = fs.get_or_create_feature_group(
        name='weather',
        description='Weather characteristics of each hour',
        version=1,
        primary_key=['datetime'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    weather_fg.insert(historical_weather_data_df)

    # historical_weather_data_df.info()
    # print(historical_weather_data_df.head())



def backfill_vehicles(fs, date, start_hour, end_hour):
    vehicle_df = vehicle_data.get_vehicle_position_data(company, date, start_hour, end_hour)
    # vehicle_df.info()
    # print(vehicle_df.head(10))

    vehicle_fg = fs.get_or_create_feature_group(
        name='vehicle',
        description='Data on vehicles',
        version=1,
        primary_key=['id'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    vehicle_fg.insert(vehicle_df)

def backfill(fs):

    date = datetime.now()
    date = date - timedelta(days=4)
    year = date.year
    month = date.month
    day = date.day

    date = f"{year}-{month}-{day}"

    start_hour = 0
    end_hour = 23

    backfill_dates(fs, year, month, day)
    backfill_weather(fs, date)
    backfill_vehicles(fs, date, start_hour, end_hour)



