import pykoda_main.src.pykoda as pk


#print(pk.geoutils.flat_distance((0.1, 0.01), (0.2, 0.3)))

company = "skane"
date = "2022-08-21"

start_hour = 15
end_hour = 17

df = pk.datautils.get_data_range(feed='TripUpdates', company=company, start_date=date, start_hour=start_hour, end_hour=end_hour, merge_static=True)

df.info()

'''
import gzip
import shutil
import tarfile

input = "skane.bz2"
output = "skane2.bz2"


with gzip.open(input) as g:
    with open(output, 'wb') as f_out:
        shutil.copyfileobj(g, f_out)


tarfile.open(input, 'r:bz2') # <tarfile.TarFile object at 0x109152690>

'''
