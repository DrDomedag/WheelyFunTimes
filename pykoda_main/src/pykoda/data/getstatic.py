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

from .. import config
from .getdata import download_file


def _get_static_data_path(company: str, date: str) -> str:
    return f'{config.CACHE_DIR}\\{company}_static_{date.replace("-", "_")}'


def get_static_data(date: str, company: str, outfolder: (str, None) = None) -> None:
    if outfolder is None:
        outfolder = _get_static_data_path(company, date)

    # admit both _ and -
    date = date.replace('_', '-')

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
    if config.API_VERSION == 1:
       koda_url = f"https://koda.linkoping-ri.se/KoDa/api/v0.1?company={company}&feed=GTFSStatic&date={date}"
    else:
        koda_url = f'https://koda.linkoping-ri.se/KoDa/api/v2/gtfs-rt/{company}/GTFSStatic?date={date}&key={os.environ["KODA_API_KEY"]}'

    download = ey.func(download_file, inputs={'url': koda_url}, outputs={'file': outfolder + '.zip'})

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

