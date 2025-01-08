# 🚍 Wheely Fun Times 🚍

Have you ever been packed like a sardine into a bus, a suitcase in your ribs and an elbow in your ear, thinking “if only I had known, I would have taken the subway instead”? Well, fear not - the Wheely Fun Times team is here with a solution for all your bus occupancy status forecast needs. Connect using the latest AI technology and never ride an overcrowded bus again!

## Table of contents

[Introduction and overview](#introduction-and-overview)

[Team introduction](#team-introduction)

[Prediction problem](#prediction-problem)

[Data](#data)

[Architecture](#architecture)

[Model](#model)

[Feature Engineering](#feature-engineering)

[GUI](#gui)

## Introduction and overview
This project was carried out as part of the course ID2223 - Scalable Machine Learning at KTH. The project objective was to create a serverless machine learning solution for a real-world problem using a live updating data source. After discussing various options and problems that machine learning could help alleviate, we decided to focus on predicting the occupancy status of buses in traffic. There are certainly people who would rather walk or take the subway if the bus is overly crowded, and accurate predictions could also help companies engaged in public transport in planning their traffic. There is also data available through the traffic operators that would provide us with a ground truth to train a machine learning model. To strengthen the model’s predictive power, we also consider data from the two factors we deemed likely to have the greatest and most consistent impact on occupancy: the calendar and the weather. To finally make the model’s predictions available for public consideration and utilisation, we developed a UI where they can be viewed. To better understand the rest of this report, we encourage you to try it out [here](https://huggingface.co/spaces/WheelyFunTimesTeam/WheelyFunTimes)!

## Team introduction
The Wheely Fun Times team consists of Ellinor Teurnberg and Joel Lindgren, two fifth year students of Industrial Engineering and Management at KTH working on their master’s degree in machine learning.

## Prediction problem
As the ground truth data from the API used came in the form of six categories from “EMPTY” to “FULL”, the data can be seen as ordinal. This means it could be solved either by regression, with the possible output values clamped to somewhere between 0 and 5 and rounded to the nearest integer, or as classification. Although it meant that the model would not inherently understand the ordinal relationship between the categories, we chose to take the latter approach for a few reasons. One advantage is that the model always serves up an unambiguous answer, rather than unclear answers such as ‘4.5’, ‘12’ or ‘-1.5’. Further, the intervals between the occupancy categories are not equal, making regression significantly less suitable.

## Data
The data used has been collected from four distinct APIs:

**OpenMeteo** - Used for collecting both historical weather data as well as weather forecasts. The location is set to Malmö.

**Svenska Dagar API 2.1** - Used to collect calendar data such as which day of the week it is, whether a given day is a Swedish holiday, etc.

**KoDa - Kollektivtrafikens Datalab** - Used to collect historical data about the vehicles. Each vehicle gives multiple updates throughout its trip, such as occupancy status, position, route name etc. The data contains information from routes across the whole Skåne Region.

**GTFS Regional** - Real time data on buses. Used to collect predicted bus trips today and (part of) tomorrow. The data contains information from routes across the whole Skåne Region.

The data is collected on demand for backfill, where OpenMeteo Historical Weather Data, Svenska Dagar API, and the KODA API are called. We have trained the model on a month of data from these APIs, specifically December 1st to December 31st. The weather data, containing hourly data points, and the calendar data, containing daily data points, is merged with the vehicle data from the KoDa API. This is updated irregularly, and contains just over 47 000 unique trips for the month of December. In total, there are almost 17 000 000 datapoints, where each datapoint corresponds to a positional update from a bus in Skåne.   

A limitation with the data used to train our model is that it is only from one month - one when the weather is overall quite bad in Sweden - due to limitations in processing power. This means that the model in its current form might generalise poorly. For example, in the summer when it is warm, people might take the bus only if it rains, and otherwise walk or take the bike. As time goes on and the model continues to be trained on season-appropriate data, it will get better at generalising to these disparate seasons and user behaviours.

In addition to long-term historical data used for model training, the daily feature pipeline fetches yesterday’s historical weather and vehicle data as well as weather predictions for today and tomorrow, the schedule for the vehicles today and tomorrow (as much as available), and the calendar data. The weather forecast and vehicle schedule data is used for inference, and the historical parts are added to long-term storage for the training of future model versions. There are approximately 1 000 000 predictions made for two days.

## Architecture
![architecture](https://github.com/user-attachments/assets/9e3d9f34-e345-4df8-a7b1-fa8b8ca8db59)

**Figure X:** An overview of the project’s software architecture

### Pipelines
Backfill - Loads historical vehicle, weather and calendar data for a desired period of time. The data is cleaned and uploaded into separate feature groups in Hugging Face.
Feature Update - Reads yesterday’s data from KoDa and Openmeteo and uploads it to the previously mentioned feature groups in Hopsworks. Data for a weather forecast and schedule for the vehicles are also uploaded to feature groups in Hopsworks. The feature update pipeline is run daily using Github actions.
Training - Load data from Hopsworks and merge the data from the feature groups so that each instance of vehicle data is mapped to the correct weather and calendar data based on the timestamp. Furthermore, model specific parameters are created. The trained model is uploaded to Hopsworks model registry along with performance metrics: precision, recall, F1 score, and accuracy.
Inference - The model is loaded from Hopsworks model registry and predictions for the current day and tomorrow. The predictions are uploaded to Hopsworks as well. The inference pipeline is run daily using GitHub actions a few minutes after the feature pipeline.

## Model
XGBoostClassification, a decision tree ensemble model, was used for training and inference. This was chosen over a deep learning based approach mainly for its speed and ease of training, leaving more time for other aspects of the project and allowing these tasks to be carried out locally or on Github Actions for speedy code iteration. If the project was to be further developed, looking at LSTM architecture would likely be an early step.

Suitable model parameters were found using the RandomizedSearchCV class from scikit-learn, although the impact on the model’s performance compared to the XGBoostClassifier class’ base values was marginal.

## Feature engineering
We did a correlation matrix to find if there were any features with high correlation that could be removed from the model. The matrix mainly indicated high correlation between the different day-features, and we decided to remove the features indicating the day of the week, whether the day was a holiday and whether it was an informal holiday or a bank holiday. Each correlated significantly to the others, and notably to the feature indicating whether the day was a labour-free day, which was kept to maintain this information.

![Figure_1](https://github.com/user-attachments/assets/f2dd9d29-49e0-4f8f-9722-f8f09a1d883f)

**Figure X:** Correlation matrix for all of the model’s features. The “Unknown” feature is a duplicate of the datetime feature, and neither was used in training.



We also tested the importance for the features in the model (gain, total gain, cover and total cover), and some of the results can be seen in Figure X. 


<p float="left">
  <img src="https://github.com/user-attachments/assets/111ebb80-a1db-4f59-abf2-502dbcd52631" width="500" />
  <img src="https://github.com/user-attachments/assets/caf749f9-8524-448a-b551-7a9e64241ae3" width="500" /> 
</p>


**Figure X:** Total Gain vs. Total Cover and Gain vs. Total Gain - note that the bus route name feature has been excluded, as its importance on all axes was so great that the other points ended up indistinguishable in one corner.

Based on these results, we tried removing the features that seemed to have the lowest overall importance to improve the model, namely which direction the bus was going in on its route, and which minute of the hour the observation was made. The direction id could possibly have been relevant if, for example, buses heading into the city were more packed in the morning than the ones leaving it, and vice versa in the afternoon, but this seemed not to meaningfully be the case. The low impact of the minute value is unsurprising.

All of the weather related features scored low in terms of impact. However, we did not want to remove these as these calculations were based on December data and we suspect that these features’ impact might be greater in the summer. This would therefore risk impacting the model’s generalisability.

Finally, as longitude and latitude seemed to have great importance, and models generally do not perform very well with raw longitudinal and latitudinal data, an effort was made to map the positions of the bus to the location of the bus stops. This would result in only having one vehicle update for every bus stop, instead of multiple ones. However, the process of mapping the locations based on haversine distance was computationally prohibitive so this is left as a possible feature improvement for the system.

The feature selection and hyperparameter optimization described above increased the accuracy from 0.655 to 0.662, and the F1 score was increased from 0.618 to 0.639. It is possible that a more thorough hyperparameter optimization could increase performance further.



## Running the software
A github action is scheduled to run the acquisition of new features on a daily basis, followed by inference. This generates occupancy forecasts for the next day or two (the limiter being the availability of schedule data from Trafiklabbet’s GTFS API). An action is also available for running backfill and for module training. These are run on demand. Each of these actions can also be run locally, should one so prefer, by calling on its respective function from the main.py file.

## GUI
A graphical user interface was created using Streamlit and hosted on Hugging Face. Check it out [here](https://huggingface.co/spaces/WheelyFunTimesTeam/WheelyFunTimes). The UI lets the user select a bus route, a day (today or tomorrow), a time window, and a bus stop. The interface then shows a graph and a map describing the occupancy status for all available trips that pass the stop within the desired time window. The first time the interface is run, predictions are loaded from Hopsworks and, to decrease loading times, are saved to a csv file. This is kept for up to 12 hours, after which the next visit will trigger a download of fresh data. Please note that the amount of data available from the API for the day after the current one varies, and if data has not been provided, you will unfortunately not find many predictions for tomorrow.

Do note that, it may seem as though the model is broken due to almost always reporting that the buses are empty or nearly empty. However, the model is actually decently accurate, and this is primarily an effect of Sweden’s low population density.
