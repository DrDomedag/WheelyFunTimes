import os
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/skane/VehiclePositions"
CHECK_INTERVAL = 60  # Interval in seconds

# Ensure the output folder exists
os.makedirs(os.environ["cache_dir"], exist_ok=True)

def fetch_data(date, hour):
    url = f"{API_BASE_URL}?date={date}&hour={hour}&key={os.environ['KODA_API_KEY']}"
    print(f"Checking URL: {url}")

    while True:
        try:
            # Send HTTP HEAD request
            response = requests.head(url)
            
            if response.status_code == 202:
                print(f"[{datetime.now()}] HTTP 202 Accepted. Retrying in {CHECK_INTERVAL} seconds...")
                time.sleep(CHECK_INTERVAL)
            elif response.status_code == 200:
                print(f"[{datetime.now()}] HTTP 200 OK. Attempting to download the file...")
                download_file(url, date, hour)
                break
            else:
                print(f"[{datetime.now()}] Unexpected response: {response.status_code}. Exiting.")
                break
        except Exception as e:
            print(f"[{datetime.now()}] Error: {e}")
            time.sleep(CHECK_INTERVAL)

def download_file(url, date, hour):
    try:
        file_path = os.path.join(os.environ["cache_dir"], f"skane-vehiclepositions-{date}-{hour}.7z")
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}. Skipping download.")
            return
        
        # Send a GET request to download the file
        response = requests.get(url, stream=True)
        if response.status_code == 200 and "application/x-7z-compressed" in response.headers.get("Content-Type", ""):
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"File downloaded successfully: {file_path}")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error while downloading the file: {e}")

def make_requests(dates):
    for date in dates:
        print(f"Processing date: {date}")
        for hour in range(24):
            print(f"Processing hour: {hour}")
            fetch_data(date, hour)
        print(f"Completed processing for date: {date}\n")