import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def visualise(fs, df=None):

    if df is None:
        df = get_data_from_hopsworks(fs)


    '''
    packed_df = df.loc[df['vehicle_occupancystatus'] > 1]

    packed_buses = list(packed_df['route_long_name'].unique())

    print(packed_buses)'''

    
    bus_route = "21"

    bus_route_df = df.loc[df['route_long_name'] == bus_route]


    # Given date, hour, and minute
    start_year = 2024
    start_month = 12
    start_day = 30
    start_hour = 9
    start_minute = 0

    # Given date, hour, and minute
    end_year = 2024
    end_month = 12
    end_day = 30
    end_hour = 10
    end_minute = 0

    bus_route_df['datetime'] = bus_route_df['datetime'].dt.tz_localize(None)

    # Create a datetime object
    start_time = datetime(start_year, start_month, start_day, start_hour, start_minute)
    end_time = datetime(end_year, end_month, end_day, end_hour, end_minute)


    bus_route_df = bus_route_df.loc[bus_route_df['datetime'] > start_time]
    bus_route_df = bus_route_df.loc[bus_route_df['datetime'] < end_time]

    trips = list(bus_route_df['trip_id'].unique())

    num_trips = len(trips)

    print(num_trips)

    # Plotting
    plt.figure(figsize=(10, 6))  # Optional: Adjust figure size
    
    for i in range(num_trips):
        trip_id = trips[i]

        single_trip_df = bus_route_df.loc[bus_route_df['trip_id'] == trip_id]
        single_trip_df.sort_values('datetime', inplace=True)
        plt.plot(single_trip_df['datetime'], single_trip_df['vehicle_occupancystatus'], marker='o', linestyle='-', label='Occupancy Status')

    plt.xlabel('Datetime')  # Label for x-axis
    plt.ylabel('Vehicle Occupancy Status')  # Label for y-axis
    plt.title('Vehicle Occupancy Status Over Time')  # Title of the plot
    plt.grid(True)  # Add grid for better readability
    plt.legend()  # Add legend
    plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility
    plt.tight_layout()  # Adjust layout to prevent clipping
    plt.show()
    


def get_data_from_hopsworks(fs):
    prediction_fg = fs.get_feature_group(
        name="predictions",
        version=1,
    )
    prediction_df = prediction_fg.read()

    return prediction_df




