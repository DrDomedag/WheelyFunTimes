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
    date_df.info()
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
    print(date)

    start_hour = 0
    end_hour = 23

    backfill_dates(fs, year, month, day)
    backfill_weather(fs, date)
    backfill_vehicles(fs, date, start_hour, end_hour)



