import pandas as pd
from numpy import float64
import datetime as dt

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

df = pd.read_csv('../data/test.csv')

#Create a timestamp out of the four columns
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