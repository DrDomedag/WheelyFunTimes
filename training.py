#Start with XGBoost as baseline¨
#Then test LSTM

import os
"""
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = "python"
"""

#from date_data import get_calendar_data
#import weather_data
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
#import pykoda_main.src.pykoda as pk
#import get_static_custom
#import hopsworks
#import vehicle_data
#import numpy as np

from hsml.schema import Schema
from hsml.model_schema import ModelSchema

from sklearn.metrics import log_loss, r2_score, accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
import xgboost as xgb
import matplotlib.pyplot as plt
from hyperopt import fmin, tpe, hp

def train(fs, mr, show_plot=False, train_from_local_data=False, upload_model=True):
    if train_from_local_data:
        training_data = pd.read_csv(os.environ["cache_dir"] + '/training_data.csv')
    else:
        training_data = get_training_data(fs)
    

    training_data = training_data.dropna()

    # Create a mapping dictionary
    availability_mapping = {
        'EMPTY': 0,
        'MANY_SEATS_AVAILABLE': 1,
        'FEW_SEATS_AVAILABLE': 2,
        'STANDING_ROOM_ONLY': 3,
        'CRUSHED_STANDING_ROOM_ONLY': 4,
        'FULL': 5
    }

    training_data['vehicle_occupancy_status'] = training_data['vehicle_occupancy_status'].map(availability_mapping)


    training_data['dag_i_vecka'] = training_data['dag_i_vecka'].astype('category')
    training_data['route_long_name'] = training_data['route_long_name'].astype('category')
    #training_data['route_id'] = training_data['route_id'].astype('category')

    training_data['vehicle_occupancy_status'] = training_data['vehicle_occupancy_status'].astype('int')
    training_data["arbetsfri_dag"] = training_data["arbetsfri_dag"].astype("bool")
    training_data["holiday"] = training_data["holiday"].astype("bool")
    training_data["helgdag"] = training_data["helgdag"].astype("bool")
    training_data["squeeze_day"] = training_data["squeeze_day"].astype("bool")
    training_data["helgdagsafton"] = training_data["helgdagsafton"].astype("bool")
    training_data["day_before_holiday"] = training_data["day_before_holiday"].astype("bool")
    #training_data["arbetsfri_dag"] = training_data["arbetsfri_dag"].astype("bool")
    
    #training_data['datetime'] = pd.to_datetime(training_data['datetime'])
    #training_data['datetime'] = training_data['datetime'].tz_localize(None)

    
    split_date = "2024-12-25 00:00:00" # ~80%
    #split_date = pd.to_datetime("2024-12-27 00:00:00")
    #split_date = np.datetime64(split_date)

    df_training = training_data.loc[training_data['datetime'] <= split_date]
    df_test = training_data.loc[training_data['datetime'] > split_date]

    train_features = df_training.drop(["id", "trip_id", "datetime", "route_short_name", "vehicle_occupancy_status"], axis=1)
    test_features = df_test.drop(["id", "trip_id", "datetime", "route_short_name", "vehicle_occupancy_status"], axis=1)

    train_labels = pd.DataFrame(df_training['vehicle_occupancy_status'])
    test_labels = pd.DataFrame(df_test['vehicle_occupancy_status'])
    
    # Creating an instance of the XGBoost Regressor
    xgb_classifier = xgb.XGBClassifier(tree_method="hist", enable_categorical=True)

    # Define the hyperparameter space
    space = {
        'max_depth': hp.quniform('max_depth', 2, 8, 1),
        'learning_rate': hp.loguniform('learning_rate', -5, -2),
        'subsample': hp.uniform('subsample', 0.5, 1)
    }

    # Define the objective function to minimize
    def objective(params):
        xgb_model = xgb_classifier
        xgb_model.fit(train_features, train_labels)
        y_pred = xgb_model.predict(test_features)
        score = accuracy_score(test_labels, y_pred)
        return {'loss': -score, 'status': "ok"}

    # Perform the optimization
    best_params = fmin(objective, space, algo=tpe.suggest, max_evals=10)
    print("Best set of hyperparameters: ", best_params)

    # Fitting the XGBoost Regressor to the training data
    xgb_classifier.fit(train_features, train_labels)

    # Predicting target values on the test set
    predicted_labels = xgb_classifier.predict(test_features)

    # Calculating Mean Squared Error (MSE) using sklearn
    
    accuracy = accuracy_score(test_labels, predicted_labels)
    precision = precision_score(test_labels, predicted_labels, average='weighted')
    recall = recall_score(test_labels, predicted_labels, average='weighted')
    f1 = f1_score(test_labels, predicted_labels, average='weighted')
    print(f"Accuracy: {accuracy}, precision: {precision}, recall: {recall}, f1 score: {f1}")

    result_df = test_labels
    result_df["vehicle_occupancyStatus"] = predicted_labels
    result_df["datetime"] = df_test["datetime"]
    result_df["trip_id"] = df_test["trip_id"]
    result_df["route_short_name"] = df_test["route_short_name"]
    result_df = result_df.sort_values(by=["datetime"])
    print(result_df.head())
    print(result_df.tail())
    result_df.info()

    # Creating a directory for the model artifacts if it doesn't exist
    model_dir = "occupancy_model"
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)

    
    plot_model(xgb_classifier, model_dir, show_plot)
    
    
    # Save model to Hopsworks

    # Creating input and output schemas using the 'Schema' class for features (X) and target variable (y)
    input_schema = Schema(train_features)
    output_schema = Schema(train_labels)

    # Creating a model schema using 'ModelSchema' with the input and output schemas
    model_schema = ModelSchema(input_schema=input_schema, output_schema=output_schema)

    # Converting the model schema to a dictionary representation
    schema_dict = model_schema.to_dict()
    # Saving the XGBoost regressor object as a json file in the model directory
    
    xgb_classifier.save_model(model_dir + "/model.json")
    res_dict = {
            "Accuracy": str(accuracy),
            "Precision": str(precision),
            "Recall": str(recall),
            "F1_score": str(f1)
        }

    if upload_model:
        # Creating a Python model in the model registry named 'air_quality_xgboost_model'

        aq_model = mr.python.create_model(
            name="bus_occupancy_xgboost_model", 
            metrics= res_dict,
            model_schema=model_schema,
            input_example=test_features.sample().values, 
            description="Bus occupancy predictor for Skane",
            version=4
        )

        # Saving the model artifacts to the 'air_quality_model' directory in the model registry
        aq_model.save(model_dir)

def plot_model(xgb_regressor, model_dir, show_plot):
    
    
    images_dir = model_dir + "/images"
    #images_dir = "images"
    if not os.path.exists(images_dir):
        os.mkdir(images_dir)
    
    '''
    file_path = images_dir + "/occupancy_model_hindcast.png"
    plt = util.plot_air_quality_forecast(city, street, df, file_path, hindcast=True) 
    plt.show()
    '''
    
    # Plotting feature importances using the plot_importance function from XGBoost
    xgb.plot_importance(xgb_regressor, max_num_features=4)
    feature_importance_path = images_dir + "/feature_importance.png"
    plt.savefig(feature_importance_path)
    if show_plot:
        plt.show()
    

def get_training_data(fs):

    weather_fg = fs.get_feature_group(
        name='weather',
        version=1,
    )

    date_fg = fs.get_feature_group(
        name="date",
        version=1,
    )

    vehicle_fg = fs.get_feature_group(
        name="vehicle",
        version=1,
    )

    #print(weather_fg.select_all().show(10))
    #print(date_fg.select_all().show(20))
    
    #weather_date_query = weather_fg.select_all().join(date_fg.select_except(["datetime"]), on=["datetime"])
    #print(weather_date_query.read().head(10))

    #date_weather_query = date_fg.select_all().join(weather_fg.select_except(["datetime"]), on=["datetime"])
    #print(date_weather_query.read().head(10))

    '''
    # Create a query for the daily feature group
    daily_query = date_fg.select_all()

    # Create a query for the hourly feature group
    hourly_query = weather_fg.select_all()

    # Join the two queries
    # Use a temporal join (assumes both have a timestamp or similar column)
    joined_query = hourly_query.join(
        daily_query, 
        left_on="datetime", 
        right_on="datetime",   # Replace date with the name of the column in the daily feature group
        join_type="leftOuter"
    )

    print(joined_query.read().show(10))
    '''

    weather_df = weather_fg.read()
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

    #print(weather_date_df.head(10))

    print("Attempting to read vehicle_fg")
    # This doesn't work - it times out once you have a month or so of data.
    # vehicle_df = vehicle_fg.read()

    periods = 12
    days_per_period = 5
    total_days = periods * days_per_period

    now = datetime.now()
    start_date = now - relativedelta(days=total_days)
    current_start_date = start_date
    current_end_date = current_start_date + relativedelta(days=days_per_period)
    vehicle_df = pd.DataFrame()
    for i in range(periods):
        print(f"Attempting to get vehicle data from {current_start_date.strftime('%Y-%m-%d')} to {current_end_date.strftime('%Y-%m-%d')}")
        vehicle_df = pd.concat([vehicle_df, vehicle_fg.filter((vehicle_fg.datetime >= current_start_date) & (vehicle_fg.datetime <= current_end_date)).read()])

        current_start_date += relativedelta(days=days_per_period)
        current_end_date += relativedelta(days=days_per_period)
    
    print("Finished reading vehicle data.")

    vehicle_df = vehicle_df.drop_duplicates()

    #print("Successfully read vehicle_fg")
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

    #print(weather_df.head(5))
    #print(weather_df.tail(5))

    return triplicate_df

    '''
    triplicate_fg = fs.get_or_create_feature_group(
        name='triplicate',
        description='Full characteristics for each vehicle update',
        version=1,
        primary_key=['datetime', 'trip_id'],
        event_time="datetime",
        # expectation_suite=weather_expectation_suite
    )

    triplicate_fg.insert(triplicate_df)
    '''
    

    '''
    triplicate_df = triplicate_df.sort_values("datetime")

    #triplicate_df.info()
    print(triplicate_df.head(10))
    print(triplicate_df.tail(10))

    triplicate_df = triplicate_df.dropna()
    #triplicate_df.info()
    
    print(triplicate_df.head(10))
    print(triplicate_df.tail(10))
    '''

    #Använd read för historisk data

