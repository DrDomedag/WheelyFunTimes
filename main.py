import pykoda_main.src.pykoda as pk
import appdirs
import os
import configparser


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

company = "skane"
date = "2022-08-21"
hour = 12

start_hour = 15
end_hour = 15

df = pk.datautils.get_data_range(feed='TripUpdates', company=company, start_date=date, start_hour=start_hour, end_hour=end_hour, merge_static=True)

df.info()

import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

df1 = df[["id", "datetime", "trip_id", "route_short_name", "stop_id", "stop_name", "scheduled_departure_time", "departure_delay"]]
df1.sort_values(["trip_id", "datetime"])
#print(df.head(20))

#df = df.loc[df['trip_id'] == "121120000307023823"]
#print(df.head(20))



df2 = pk.datautils.get_data_range(feed='VehiclePositions', company=company, start_date=date, start_hour=start_hour, end_hour=end_hour, merge_static=True)
df2 = df2[["id", "trip_id", "vehicle_id", "datetime", "route_short_name", "vehicle_occupancyStatus"]]
df2.sort_values(["trip_id", "datetime"])

unique_ids = list(df2["trip_id"].unique())

final_df = pd.DataFrame()

import random
random.shuffle(unique_ids)

for unique_id in unique_ids:

    first = unique_id
    #print(first)
    first_df = df2.loc[df2["trip_id"] == first]
    #first_df["timestamp"] = pd.to_datetime(first_df["timestamp"])
    #print(first_df.head(10))

    #first_df.set_index("timestamp", inplace=True)

    # Resample and take the first value for each minute
    #first_df['timestamp'] = pd.to_datetime(first_df['timestamp'])
    result = (
        first_df.groupby(first_df['datetime'].dt.floor('T'))
        .first()
        .rename_axis('datetime_minute')
        .reset_index()
    )
    #result = result.set_index(['datetime_minute'])

    #result.info()
    #print(result.head(5))

    trip_df = df1.loc[df1["trip_id"]==first]
    #print(trip_df.head())

    trip_df.info()
    print(trip_df.head(10))

    merged_df = pd.merge_asof(result, trip_df, left_on="datetime_minute", right_on="datetime")
    #merged_df.info()
    #print(merged_df.head(5))


    merged_df = merged_df.query('id_y.notna()', engine='python')
    merged_df = merged_df.query('datetime_y.notna()', engine='python')
    merged_df = merged_df.query('trip_id_y.notna()', engine='python')
    merged_df = merged_df.query('route_short_name_y.notna()', engine='python')
    merged_df = merged_df.query('stop_id.notna()', engine='python')
    merged_df = merged_df.query('stop_name.notna()', engine='python')
    merged_df = merged_df.query('scheduled_departure_time.notna()', engine='python')
    merged_df = merged_df.query('departure_delay.notna()', engine='python')
    merged_df = merged_df.query('vehicle_occupancyStatus.notna()', engine='python')

    merged_df = merged_df.loc[merged_df["vehicle_occupancyStatus"] != "None"]

    #print(merged_df.head(5))

    pd.concat([final_df, merged_df])

final_df.info()
print(final_df.head())


'''
print("-----------")
print(first_df.head())
filtered_df = first_df.loc[first_df["timestamp"].dt.second == 0]
print(first_df.head())
'''

"""first_df["minute"] = first_df["timestamp"].dt.floor("T")
filtered_df = first_df.groupby("minute").first().resetIndex()
filtered_df = filtered_df.drop(columns=["minute"])
print(filtered_df.head())
"""


#df3 = pd.merge(df1, df2, on=["trip_id", "datetime"])
#df3.info()
#print(df3.head(5))

#df3 = df3.loc[df3['trip_id'] == "121120000307023823"]
#print(df3.head(20))

'''
df.info()
df = df[["vehicle_id", "id", "datetime", "route_short_name", "vehicle_occupancyStatus"]]
df = df.loc[df['vehicle_id'] == "9031012002204027"]
df = df[["vehicle_id", "id", "datetime", "vehicle_occupancyStatus"]]

print(df.head(20))
'''


'''
unique_service_ids = list(df['route_short_name'].unique())
print(len(unique_service_ids))
print(unique_service_ids)

# Remove all data without occupancy status data
df = df.query('vehicle_occupancyStatus.notna()', engine='python')

df.info()

unique_service_ids_post = list(df['route_short_name'].unique())
print(len(unique_service_ids_post))
print(unique_service_ids_post)

#pk.datautils.getdata.get_data(date, hour, "TripUpdates", "skane")

missing_list = []
found_list = []

for value in unique_service_ids:
    if value not in unique_service_ids_post:
        missing_list.append(value)
    else:
        found_list.append(value)

print("Missing list:")
print(missing_list)
print("Found list:")
print(found_list)
'''
