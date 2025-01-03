import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBClassifier
from xgboost import plot_importance
import matplotlib.pyplot as plt
import datetime
import util
import merge_with_stops

def get_data(fs):
    weather_fg = fs.get_feature_group(
        name='weather',
        version=1,
    )
    date = datetime.datetime.now() - datetime.timedelta(days=1)
    batch_data = weather_fg.filter(weather_fg.datetime >= date).read()


    date_fg = fs.get_feature_group(
        name="date",
        version=1,
    )

    vehicle_fg = fs.get_feature_group(
        name="vehicle_future",
        version=1,
    )

    weather_df = batch_data
    date_df = date_fg.filter(date_fg.datetime >= date).read()    

    weather_df = weather_df.sort_values('datetime')
    date_df = date_df.sort_values('datetime')


    # Perform a merge_asof with tolerance to nearest daily match
    weather_date_df = pd.merge_asof(
        weather_df,
        date_df,
        on='datetime',
        direction='backward',  # Matches the nearest earlier daily timestamp
        tolerance=pd.Timedelta('1d')  # Allow matching within 1 day
    )
    
    weather_date_df = weather_date_df.dropna()

    vehicle_df = vehicle_fg.filter(vehicle_fg.datetime >= date).read()
    vehicle_df = vehicle_df.sort_values("datetime")
    #vehicle_df["datetime"] = pd.to_datetime(vehicle_df["datetime"])

    weather_df.info()
    date_df.info()
    vehicle_df.info()

    triplicate_df = pd.merge_asof(
        vehicle_df,
        weather_date_df,
        on='datetime',
        direction='backward',
        tolerance=pd.Timedelta('1d')
    )

    triplicate_df["hour"] = triplicate_df["datetime"].dt.hour
    triplicate_df["minute"] = triplicate_df["datetime"].dt.minute

    triplicate_df.info()
    ids = list(pd.unique(triplicate_df["trip_id"]))
    id = ids[300]
    show_df = triplicate_df[triplicate_df["trip_id"]==id]
    print(show_df.head())
    print(show_df.tail())

    return triplicate_df
        

def upload_result_to_hopsworks(fs, df):
    predictions_fg = fs.get_or_create_feature_group(
        name='predictions',
        description='Predicted occupancy',
        version=1,
        primary_key=['datetime', 'trip_id'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    predictions_fg.insert(df)


def inference(fs, mr):
    util.delete_feature_groups(fs, "predictions")

    pred_df = get_data(fs)

    pred_df = pred_df.dropna()

    pred_df['dag_i_vecka'] = pred_df['dag_i_vecka'].astype('category')
    pred_df['route_long_name'] = pred_df['route_long_name'].astype('category')

    pred_df["arbetsfri_dag"] = pred_df["arbetsfri_dag"].astype("bool")
    pred_df["holiday"] = pred_df["holiday"].astype("bool")
    pred_df["helgdag"] = pred_df["helgdag"].astype("bool")
    pred_df["squeeze_day"] = pred_df["squeeze_day"].astype("bool")
    pred_df["helgdagsafton"] = pred_df["helgdagsafton"].astype("bool")
    pred_df["day_before_holiday"] = pred_df["day_before_holiday"].astype("bool")

    #pred_features = pred_df.drop(["trip_id", "datetime", "route_short_name"], axis=1)
    
    pred_features = pred_df[['vehicle_position_latitude', 'vehicle_position_longitude', 'route_long_name', 'direction_id', 'temperature_2m', 'precipitation', 'wind_speed_10m', 'hourly_cloud_cover', 'dag_i_vecka', 'arbetsfri_dag', 'holiday', 'helgdag', 'squeeze_day', 'helgdagsafton', 'day_before_holiday', 'hour', 'minute']]

    retrieved_model = mr.get_model(
        name="bus_occupancy_xgboost_model",
        version=1,
    )

    # Download the saved model artifacts to a local directory
    saved_model_dir = retrieved_model.download()

    # Loading the XGBoost regressor model and label encoder from the saved model directory
    # retrieved_xgboost_model = joblib.load(saved_model_dir + "/xgboost_regressor.pkl")
    retrieved_xgboost_model = XGBClassifier(tree_method="hist", enable_categorical=True)

    retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")

    # Displaying the retrieved XGBoost regressor model
    #retrieved_xgboost_model

    pred_labels = retrieved_xgboost_model.predict(pred_features)

    result_df = pred_df
    result_df["vehicle_occupancyStatus"] = pred_labels
    result_df = result_df.sort_values(by=["datetime"])

    #Merge with stops to add the stop name

    merged_df = merge_with_stops.merge_exact(fs, result_df)

    upload_result_to_hopsworks(fs, merged_df)

    return merged_df

    

