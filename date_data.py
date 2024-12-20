import pandas as pd
import requests

URL_BASE = "http://sholiday.faboul.se/dagar/v2.1"

def get_JSON_for_date(year, month=None, day=None):
    if month is None:
        url = f"{URL_BASE}/{year}"
    elif day is None:
        url = f"{URL_BASE}/{year}/{month}"
    else:
        url = f"{URL_BASE}/{year}/{month}/{day}"
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails

    # Step 2: Parse the JSON content
    json_data = response.json()["dagar"]


    return json_data



pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

year = "2015"
month = "01"
day = "06"

json_data = get_JSON_for_date(year, month)

df = pd.DataFrame(json_data)

df.info()
print(df.head(10))
