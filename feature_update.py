import weather_data


def get_weather_forecast(fs, city, latitude, longitude):
    weather_data_forecast_df = weather_data.get_hourly_weather_forecast(city, latitude, longitude)

    weather_fg = fs.get_or_create_feature_group(
        name='weather',
        description='Weather characteristics of each hour',
        version=1,
        primary_key=['date'],
        event_time="date",
        # expectation_suite=weather_expectation_suite
    )

    # weather_data_forecast_df.info()
    # print(weather_data_forecast_df.head())

    weather_fg.insert(weather_data_forecast_df)

    # weather_data_forecast_df.info()
    # print(weather_data_forecast_df.head())