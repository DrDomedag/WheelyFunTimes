import pykoda_main.src.pykoda as pk


#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))

company = "skane"
date = "2020-08-21"

start_hour = 15
end_hour = 19

df = pk.datautils.get_data_range(feed='TripUpdates', company=company, start_date=date,
                                         start_hour=start_hour, end_hour=end_hour, merge_static=True)

df.info()


