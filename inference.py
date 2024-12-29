import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBClassifier
from xgboost import plot_importance
import matplotlib.pyplot as plt
import datetime

def get_data(fs):
    weather_fg = fs.get_feature_group(
        name='weather',
        version=1,
    )
    today = datetime.datetime.now() - datetime.timedelta(0)
    batch_data = weather_fg.filter(weather_fg.datetime >= today).read()

    date_fg = fs.get_feature_group(
        name="date",
        version=1,
    )

    vehicle_fg = fs.get_feature_group(
        name="vehicle_future",
        version=1,
    )

    weather_df = batch_data
    date_df = date_fg.read()

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

    vehicle_df = vehicle_fg.read()
    vehicle_df = vehicle_df.sort_values("datetime")
    vehicle_df["datetime"] = pd.to_datetime(vehicle_df["datetime"])

    

    triplicate_df = pd.merge_asof(
        vehicle_df,
        weather_date_df,
        on='datetime',
        direction='backward',
        tolerance=pd.Timedelta('1d')
    )

    triplicate_df["hour"] = triplicate_df["datetime"].dt.hour
    triplicate_df["minute"] = triplicate_df["datetime"].dt.minute

    return triplicate_df
        

def inference(fs, mr):
    pred_df = get_data(fs)

    pred_df = pred_df.dropna()

    pred_df['dag_i_vecka'] = pred_df['dag_i_vecka'].astype('category')
    pred_df['route_short_name'] = pred_df['route_short_name'].astype('category')

    pred_features = pred_df.drop(["trip_id", "datetime"], axis=1)

    pred_features.info()
    
    pred_features = pred_features[['vehicle_position_latitude', 'vehicle_position_longitude', 'route_short_name', 'direction_id', 'temperature_2m', 'precipitation', 'wind_speed_10m', 'hourly_cloud_cover', 'dag_i_vecka', 'arbetsfri_dag', 'holiday', 'helgdag', 'squeeze_day', 'helgdagsafton', 'day_before_holiday', 'hour', 'minute']]

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


    #Se att det funkar
    #Välja hur vi ska titta på det - per buss? 
    #Spara till hopsworks

    








