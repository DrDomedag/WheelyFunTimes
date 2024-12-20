import pandas as pd
import requests

URL_BASE = "http://sholiday.faboul.se/dagar/v2.1"

def get_calendar_data(year, month=None, day=None):
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

    df = pd.DataFrame(json_data)


    final_columns = ['datum', 'dag i vecka', 'arbetsfri dag', 'röd dag', 'helgdag', 'klämdag', 'helgdagsafton', 'dag före arbetsfri helgdag']
    for column in final_columns:
        if column not in df.columns:
            df[column] = None

    columns_to_transform = ['helgdag', 'klämdag', 'helgdagsafton', 'dag före arbetsfri helgdag']

    # Apply the transformation to the specified columns
    df[columns_to_transform] = df[columns_to_transform].notna().astype(int)


    df['arbetsfri dag'] = df['arbetsfri dag'].map({'Ja': 1, 'Nej': 0})
    df['röd dag'] = df['röd dag'].map({'Ja': 1, 'Nej': 0})


    df = df[final_columns]

    return df



pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

year = "2015"
month = "01"
day = "06"

df = get_calendar_data(year, month, day)

#df.info()
#print(df.head(10))
