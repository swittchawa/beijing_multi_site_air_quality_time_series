from email import message
import pandas as pd
import datetime as dt
from numpy import float64

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# Read the csv into a dataframe
df = pd.read_csv('../data/PRSA_Data_Aotizhongxin_20130301-20170228.csv')

# Create a timestamp out of the four columns
# needed for influx 2020-01-01T00:00:00.00Z
# lambda s : dt.datetime(*s) take every row and parses it -> *s
# strftime to reformat the string into influxdb format
df['TimeStamp'] = df[['year', 'month', 'day', 'hour']].apply(lambda s: dt.datetime(*s).strftime('%Y-%m-%dT%H:%M:%SZ'), axis=1)

# Set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace=True)
# Drop the year, month, day, hour, No from the dataframe
converted_ts = df.drop(['year', 'month', 'day', 'hour', 'No'], axis = 1)
print(converted_ts)

# Change the column types to float
ex_df = converted_ts.astype({
                "PM2.5": float64,
                "PM10": float64,
                "SO2": float64,
                "NO2": float64,
                "CO": float64,
                "O3": float64,
                "TEMP": float64,
                "PRES": float64,
                "DEWP": float64,
                "RAIN": float64,
                "WSPM": float64 })

# Define tag field
Fields = ['PM2.5', 'PM10', 'SO2', 'CO', 'O3', 'TEMP', 'PRES', "DEWP", 'RAIN', 'wd', 'WSPM']
datatags = ['station', 'wd']

client = influxdb_client.InfluxDBClient(
    url='http://localhost:8086',
    token='ZQG_Lptfwbtp555Ix267efmxra1473qygW5lQIj_YMwe-XVR714BIbUtQ6gLbBsFGHDMyhbHA0w17xwk7TB9vA==',
    org='my-org'
)

# Write the data with two tages
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='air-quality', org='my-org', record=ex_df, data_frame_measurement_name='full-tag', data_frame_tag_columns=['station', 'wd'])
print(message)

write_api.flush()

# Write the data with one tages
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='air-quality', org='my-org', record=ex_df, data_frame_measurement_name='location-tag-only', data_frame_tag_columns=['station'])
print(message)

write_api.flush()