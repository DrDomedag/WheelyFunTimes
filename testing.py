import os

import hopsworks

# Bestäm var vi vill lagra key
with open('HOPSWORKS_API_KEY.txt', 'r') as file:
    os.environ["HOPSWORKS_API_KEY"] = file.read().rstrip()

project = hopsworks.login(project="id2223AirQuality")

fs = project.get_feature_store()

fg = fs.get_feature_group(
    name='weather',
    version=1,
    )

fg.materialization_job.run(args="-op offline_fg_materialization -path hdfs:///Projects/id2223AirQuality/Resources/jobs/weather_1_offline_fg_materialization/config_1735567507798")