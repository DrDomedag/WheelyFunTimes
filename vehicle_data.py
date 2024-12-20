import pykoda_main.src.pykoda as pk
import pandas as pd

def get_vehicle_position_data(company, date, start_hour, end_hour):

    vehicle_position_df = pk.datautils.get_data_range(feed='VehiclePositions', company=company, start_date=date, start_hour=start_hour, end_hour=end_hour, merge_static=True)

    #df2 = df2[["id", "trip_id", "datetime", "route_short_name", "vehicle_occupancyStatus", "direction_id", "stop_sequence", "stop_id"]]
    vehicle_position_df = vehicle_position_df[["id", "trip_id", "datetime", "vehicle_position_latitude", "vehicle_position_longitude", "route_short_name", "vehicle_occupancyStatus", "direction_id"]]
    vehicle_position_df = vehicle_position_df.sort_values(["trip_id", "datetime"])

    #print(vehicle_position_df.head(10))

    # Convert `datetime` column to datetime if it's not already
    vehicle_position_df['datetime'] = pd.to_datetime(vehicle_position_df['datetime'])
    vehicle_position_df['datetime_floor'] = vehicle_position_df['datetime'].dt.floor('T')

    vehicle_position_df = (
        vehicle_position_df.groupby(['trip_id', 'datetime_floor'])
        .first()
        .reset_index()
        .rename_axis(columns=None)
    )

    vehicle_position_df = vehicle_position_df.drop("datetime_floor", axis=1)

    vehicle_position_df = vehicle_position_df.sort_values("datetime")

    return vehicle_position_df

def get_trip_data(company, date, start_hour, end_hour):
    trip_update_df = pk.datautils.get_data_range(feed='TripUpdates', company=company, start_date=date,
                                                 start_hour=start_hour, end_hour=end_hour, merge_static=True)

    trip_update_df = trip_update_df[["datetime", "trip_id", "stop_id", "scheduled_departure_time", "departure_delay"]]
    trip_update_df = trip_update_df.sort_values(["trip_id", "datetime"])
    trip_update_df['actual_departure_time'] = trip_update_df['scheduled_departure_time'] + pd.to_timedelta(
        trip_update_df['departure_delay'], unit='s')
    trip_update_df = trip_update_df.sort_values(['scheduled_departure_time', 'datetime'])

def get_unique_ids(dataframe):
    return list(dataframe["trip_id"].unique())




'''
final_columns = ["id", "trip_id", "datetime", "vehicle_position_latitude", "vehicle_position_longitude", "route_short_name", "vehicle_occupancyStatus", "direction_id", "stop_id", "scheduled_departure_time", "departure_delay"]

final_df = pd.DataFrame(columns=final_columns)

#import random
#random.shuffle(unique_trip_ids)

for unique_id in unique_trip_ids:
    trip_df = trip_update_df.loc[trip_update_df['trip_id'] == unique_id]
    vpos_df = vehicle_position_df.loc[vehicle_position_df['trip_id'] == unique_id]

    df_to_merge = pd.DataFrame(columns=final_columns)
    if trip_df.shape[0] < 1:
        print(f"No trip_df data found for trip id {unique_id}")
    else:
        df_to_merge = pd.merge_asof(
            vpos_df.sort_values('datetime'),  # Sort vpos_df by timestamp
            trip_df,
            left_on='datetime',
            right_on='scheduled_departure_time',
            direction='backward'  # Find closest preceding value
        )
    print(df_to_merge.head(10))
    #df_to_merge = pd.concat([df_to_merge, merged_df], ignore_index=True)
    final_df = pd.concat([final_df, df_to_merge], ignore_index=True)

final_df.info()
print(final_df.head(10))
'''



'''
for unique_id in unique_trip_ids:

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
