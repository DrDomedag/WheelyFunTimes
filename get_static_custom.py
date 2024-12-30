'''
This module is used to download the  GTFSStatic data from the public KoDa API.

Supported companies:
- dintur - Västernorrlands län: Only GTFSStatic
- dt - Dalatrafik
- klt - Kalmar länstrafik
- krono - Kronobergs Länstrafik: Only GTFSStatic
- otraf - Östgötatrafiken
- sj - SJ + Snälltåget + Tågab: Only GTFSStatic
- skane - Skånetrafiken
- sl - Stockholm län: All feeds without VehiclePositions
- ul - Uppsala län
- varm - Värmlandstrafik+Karlstadbuss
- vt - Västtrafik: Only GTFSStatic
- xt - X-trafik


Supported date format: YYYY-MM-DD
'''
import os

import ey

from pykoda_main.src.pykoda import config
from pykoda_main.src.pykoda.data.getdata import download_file
import pandas as pd
import datetime
import warnings
import collections
import functools

static_data = collections.namedtuple('StaticData', ['stop_times', 'stops', 'trips', 'shapes', 'routes'])


def _remove_unused_stations(stops_data: pd.DataFrame, stops_times: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    parent_stations = stops_data.parent_station.dropna().values
    used_stations = set(stops_times.stop_id).union(parent_stations)
    stops_times = stops_times.query('stop_id in @used_stations')
    stops_data = stops_data.query('stop_id in @used_stations')
    return stops_data, stops_times


def _get_static_data_path(company: str) -> str:
    return f'{config.CACHE_DIR}\\{company}_static_rt'


def get_static_data(company: str, outfolder: (str, None) = None) -> None:
    if outfolder is None:
        outfolder = _get_static_data_path(company)

    if os.path.exists(outfolder):
        return
    # ------------------------------------------------------------------------
    # Create data dir
    # ------------------------------------------------------------------------
    #ey.shell('mkdir -p {outfolder}'.format(outfolder=outfolder))

    if not os.path.exists(outfolder):
        os.makedirs(outfolder)

    # ------------------------------------------------------------------------
    # Download data
    # ------------------------------------------------------------------------
    # 
    url = f"https://opendata.samtrafiken.se/gtfs/{company}/{company}.zip?key={os.environ['GTFS_STATIC_KEY']}"

    download = ey.func(download_file, inputs={'url': url}, outputs={'file': outfolder + '.zip'})

    with open(download.outputs['file'], 'rb') as f:
        start = f.read(10)
        if b'error' in start:
            msg = start + f.read(70)
            msg = msg.strip(b'{}" ')
            raise ValueError('API returned the following error message:', msg)

    # ------------------------------------------------------------------------
    # Extract .zip bz2 archive
    # ------------------------------------------------------------------------
    #untar = ey.shell((
    #    'unzip -d {outfolder} {archive}'.format(outfolder=outfolder, archive=download.outputs['file'])))

    # Ensure the output folder exists
    os.makedirs(outfolder, exist_ok=True)

    import magic

    file_path = download.outputs['file']
    file_type = magic.from_file(file_path)
    mime_type = magic.from_file(file_path, mime=True)

    #print(f"path: {file_path}")

    #print(f"File type: {file_type}")
    #print(f"MIME type: {mime_type}")

    if file_type == "Java archive data (JAR)":
        import zipfile

        with zipfile.ZipFile(file_path, 'r') as jar:
            # Extract all files to the output folder
            jar.extractall(outfolder)
            #print(f"Extracted JAR contents to {outfolder}")

    else:
        import py7zr

        # Extract the .7z file
        archive_path = download.outputs['file']
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extractall(path=outfolder)

        #print(f"Extracted 7zip contents to {outfolder}")



@functools.lru_cache(1)
def load_static_data(company: str, date: str, remove_unused_stations: bool = False) -> (static_data, None):
    """Load static data from the cache, downloading it if necessary.

    Date should be given in the format YYYY-MM-DD or YYYY_MM_DD.
    """
    output_folder = _get_static_data_path(company)
    if not os.path.isdir(output_folder):
        try:
            get_static_data(company=company)
        except ValueError:
            print("FEL HÄR")
            warnings.warn(RuntimeWarning(f'Missing static data for {company} at {date}'))
            return

    if not os.path.exists(os.path.join(output_folder, 'stop_times.txt')):
        print("HÄR")
        warnings.warn(RuntimeWarning(f'Missing static data for {company} at {date}'))
        return

    def parse_date(time_code: str):
        time_code = list(map(int, time_code.split(':')))
        if time_code[0] < 24:
            return datetime.datetime(*map(int, date.replace('_', '-').split('-')), *time_code)
        else:
            # If it is over 24 h, roll over the next day.
            time_code[0] -= 24
            dt = datetime.datetime(*map(int, date.replace('_', '-').split('-')), *time_code)
            return dt + datetime.timedelta(days=1)

    stops_times = pd.read_csv(os.path.join(output_folder, 'stop_times.txt'), dtype={'trip_id': 'str', 'stop_id': 'str'},
                              parse_dates=['arrival_time', 'departure_time'], date_parser=parse_date)
    stops_data = pd.read_csv(os.path.join(output_folder, 'stops.txt'),
                             dtype={'stop_id': 'str', 'parent_station': 'str'})
    trips_data = pd.read_csv(os.path.join(output_folder, 'trips.txt'), dtype={'trip_id': 'str', 'route_id': 'str', "direction_id": "int64"})
    shapes_data = pd.read_csv(os.path.join(output_folder, 'shapes.txt'))
    routes = pd.read_csv(os.path.join(output_folder, 'routes.txt'), dtype={'route_id': 'str', 'agency_id': 'str',
                                                                           'route_short_name': 'str',
                                                                           'route_long_name': 'str'})

    if remove_unused_stations:
        stops_data, stops_times = _remove_unused_stations(stops_data, stops_times)

    stop_times = pd.merge(stops_times, stops_data, on='stop_id', how='left', validate='m:1')
    trips_data = pd.merge(trips_data, routes, on='route_id', how='left', validate='m:1')

    # Set indexes for faster querying and merging
    trips_data.set_index(['trip_id', 'route_id'], inplace=True, drop=True, verify_integrity=True)
    stop_times.set_index(['trip_id', 'stop_id', 'stop_sequence'], inplace=True, drop=True, verify_integrity=True)
    shapes_data.set_index(['shape_id', 'shape_pt_sequence'], inplace=True, drop=True, verify_integrity=True)
    stops_data.set_index('stop_id', inplace=True, drop=True, verify_integrity=True)

    data = static_data(stop_times=stop_times, stops=stops_data, trips=trips_data, shapes=shapes_data, routes=routes)
    return data
