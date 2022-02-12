from locale import normalize
import requests
import json
import pandas as pd
from pandas import json_normalize

with open("api_config.json") as json_data_file:
    config = json.load(json_data_file)

payload = {'Key': config['Key'], 'q': 'Bangkok', 'aqi': 'yes'}
r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

# get the json from the request's result
r_string = r.json()

# print the original json
print(r_string)

# Show the unnormalised dataframe problem
#df = pd.DataFrame.from_dict(r_string, orient='index')
#print(df)

# Flatten with normalise function
normalised = json_normalize(r_string)

# print normalised version + datatypes
print(normalised)
print(normalised.dtypes)