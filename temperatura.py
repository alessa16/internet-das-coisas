import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": -5.6344,
	"longitude": -35.4256,
	"hourly": ["temperature_2m", "rain"]
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_rain = hourly.Variables(1).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["rain"] = hourly_rain

hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe)

import paho.mqtt.client as mqtt
import requests_cache
import pandas as pd
from retry_requests import retry
import json
import time

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)

@retry(stop_max_attempt_number=5, wait_fixed=200)
def get_weather_data(url, params):
    response = cache_session.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# Define the MQTT client and connect to the broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("weather/data")

# Define the MQTT publish function
def publish_weather_data(client, weather_data):
    payload = json.dumps(weather_data)
    client.publish("weather/data", payload)

def main():
    MQTT_BROKER = "192.168.56.101"
    MQTT_TOPIC = "test_channel"
    username = "mosquitto"
    password = "dietpi"


    # Define the API endpoint and parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -5.6344,
        "longitude": -35.4256,
        "hourly": ["temperature_2m", "rain"],
        "timezone": "Ceará Mirim"
    }

    # Fetch the weather data
    weather_data = get_weather_data(url, params=params)

    # Process hourly data
    hourly_data = weather_data['hourly']
    hourly_time = pd.to_datetime(hourly_data['time'])
    hourly_temperature_2m = hourly_data['temperature_2m']
    hourly_rain = hourly_data['rain']

    # Create a dictionary to hold the processed data
    weather_info = {
        "time": hourly_time.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        "temperature_2m": hourly_temperature_2m,
        "rain": hourly_rain
    }

    # Initialize MQTT client and connect to the broker
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, 60)

    # Publish weather data
    publish_weather_data(client, weather_info)

    # Loop forever, waiting for incoming messages (optional)
    client.loop_forever()

if __name__ == "__main__":
    main()
