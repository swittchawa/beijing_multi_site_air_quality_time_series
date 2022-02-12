from numpy import float64, int32, string_
from pandas.core.reshape.pivot import pivot
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# load the configuration from the json file
with open("api_config.json") as json_data_file:
    config = json.load(json_data_file)

payload = {"Key": config['Key'], 'q': "Bangkok", 'aqi': 'no'}
r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

# Get the json
r_string = r.json()
print(r_string)

# Normalised json
normalised = json_normalize(r_string)
print(normalised)

# Timestamp format with +7
normalised['TimeStamp'] = normalised['location.localtime_epoch'].apply(lambda s: dt.datetime.fromtimestamp(s).strftime("%Y-%m-%dT%H:%M:%S+07:00"))
normalised.rename(columns={
    'location.name': 'location',
    'location.region': 'region',
    'current.temp_c': 'temp_c',
    'current.wind_kph': 'wind_kph'
}, inplace=True)
print(normalised)
print(normalised.dtypes)

normalised.set_index("TimeStamp", inplace=True)
ex_df = normalised.filter(['temp_c', 'wind_kph'])

print(ex_df)
print(ex_df.dtypes)

client = influxdb_client.InfluxDBClient(
    url='http://localhost:8086',
    token='ZQG_Lptfwbtp555Ix267efmxra1473qygW5lQIj_YMwe-XVR714BIbUtQ6gLbBsFGHDMyhbHA0w17xwk7TB9vA==',
    org='my-org'
)

# Write the test data into measurement
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='live_weather',org='my-org',record = ex_df, data_frame_measurement_name = 'api')
write_api.flush()
print(message)