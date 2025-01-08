#Ta in två dataframes
#den ena är stops?
#Den andra måste innehålla longitud och latitud
#Merge på longitud och latitud
def merge_exact(fs, bus_df):

    bus_df.info()
    print(bus_df.tail(10))

    stops_fg = fs.get_feature_group(
        name='stops',
        version=1,
    )

    bus_df = bus_df.rename(columns={"vehicle_position_latitude":"stop_lat", "vehicle_position_longitude":"stop_lon"})

    stop_df = stops_fg.read()

    #bus_df = bus_df.drop_duplicates(["datetime", "trip_id"])
    stop_df = stop_df.drop_duplicates()
    
    merged_df = bus_df.merge(stop_df, how="left", on = ["stop_lat", "stop_lon"])

    #merged_df.info()
    #print(merged_df.tail(10))

    

    return merged_df
