import pandas as pd
import numpy as np
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


def get_single_day_vehicle_data(yesterday_string, date):
    #Titta på dagens datum
    #Plocka data från koda från igår
    company = "skane"
    
    print(f"Attempting to run make requests for {date}")
    prerequestfiles.make_requests([date])
    print("Done with making requests")

    vehicle_df, static_data = vehicle_data.get_vehicle_position_data(company, yesterday_string, 15, 16)

    vehicle_df["direction_id"] = vehicle_df["direction_id"].astype(bool)

    vehicle_df.info()
    #vehicle_df = vehicle_df.rename(columns={"vehicle_position_latitude":"stop_lat", "vehicle_position_longitude":"stop_lon"})
    return vehicle_df, static_data

def get_stops(fs):
    stops_fg = fs.get_feature_group(
        name='stops',
        version=1,
    )

    stop_df = stops_fg.read()
    return stop_df


# Haversine function to calculate distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

#get_weather_forecast
#get_dates()
def merge_stop(fs):
    now = datetime.now()
    yesterday = now - timedelta(days = 5)
    day = yesterday.day
    month = yesterday.month
    year = yesterday.year

    yesterday_string = yesterday.strftime("%Y-%m-%d")
    vehicle_df, static_data = get_single_day_vehicle_data(yesterday_string, yesterday)
    test_trip_id = list(pd.unique(vehicle_df["trip_id"]))[200]
    vehicle_df = vehicle_df[vehicle_df["trip_id"] == test_trip_id]
    #vehicle_df.info()
    #stop_df = get_stops(fs)
    #stop_df.info()

    stop_times = static_data.stop_times.reset_index()

    relevant_stops = stop_times[stop_times["trip_id"] == test_trip_id]
    #relevant_stops = relevant_stops.unique("stop_id")
    
    #relevant_stops = relevant_stops.merge(static_data.stops, on='stop_id', how='left')
    #relevant_stops.info()
    #print(relevant_stops.head())
    '''
    print("Pre-filtering")
    stop_df.info()
    stop_df = stop_df[(stop_df["stop_lon"] == relevant_stops["stop_lon"]) & (stop_df["stop_lat"] == relevant_stops["stop_lat"])]
    print("Post-filtering")
    stop_df.info()
    '''

    # Expand each bus and stop into a cross-product
    bus_stop_pairs = vehicle_df.assign(key=1).merge(relevant_stops.assign(key=1), on='key').drop('key', axis=1)
    
    
    specific = bus_stop_pairs[bus_stop_pairs["stop_name"] == "Höganäs Busstorget"]
    print(specific[["vehicle_position_latitude", "vehicle_position_longitude"]].head(20))
    # Calculate distance for each pair
    bus_stop_pairs['distance'] = bus_stop_pairs.apply(
        lambda row: haversine(row['vehicle_position_latitude'], row['vehicle_position_longitude'], row['stop_lat'], row['stop_lon']),
        axis=1
    )

    print(bus_stop_pairs[["stop_name", "stop_lat", "vehicle_position_latitude", "stop_lon", "vehicle_position_longitude", "distance"]].head(20))
    # Find the closest bus for each stop
    min_indices = bus_stop_pairs.groupby('stop_name')['distance'].idxmin()
    print(min_indices)
    closest_buses = bus_stop_pairs.loc[bus_stop_pairs.groupby('stop_name').distance.idxmin()]


    # Clean up the resulting dataframe
    result = closest_buses.rename(columns={
        'vehicle_position_latitude': 'bus_latitude',
        'vehicle_position_longitude': 'bus_longitude',
        'stop_lat': 'stop_latitude',
        'stop_lon': 'stop_longitude',
    })

    # Välj ut rätt data
    result = result[['stop_name', 'stop_latitude', 'stop_longitude', 'trip_id_x', 'bus_latitude', 'bus_longitude', 'distance']]

    print("Result")
    result.info()
    print(result.head())
