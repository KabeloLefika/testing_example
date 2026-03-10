import requests

API_KEY = "5865cbb236e246e0b0c813e6b42573a5"
url="http://api.openweathermap.org/data/2.5/weather"

params = {
    "lat":-33.9249,
    "lon":18.4241,
    "appid": API_KEY,
    "units": "metric"
}
respomse = requests.get(url, params=params)
print(respomse.json())

data = respomse.json()

print(data["main"]["temp"])
print(data["weather"][0]["description"])
print(data["name"])

import pandas as pd

weather_records = {
    "temperature":data["main"]['temp'],
    "description": data["weather"][0]["description"],
    "humidity": data["main"]["humidity"],
    "wind_speed": data["wind"]["speed"],
    "city": data["name"]
}

df = pd.DataFrame([weather_records])

df.to_csv("data/weather_data.csv", index=False)

print("Saved")

airbnb_data = pd.read_csv("data/listings.csv")
print(airbnb_data.head())
print(airbnb_data.shape)
print(airbnb_data.columns.tolist())

columns_needed = ["id", "name", "host_id", "host_name",  "latitude", "longitude", "room_type", "price", "minimum_nights", 
                  "number_of_reviews", "last_review","review_scores_rating","property_type","neighbourhood_cleansed", 
                  "review_scores_location", "availability_365"]

clean_df = airbnb_data[columns_needed]

print(clean_df.head())
print(clean_df.shape)

clean_df.to_csv("data/clean_listings.csv", index=False)

print("Saved new clean listings data")

#imporing sqlalchemy to connect to the database
from sqlalchemy import create_engine

engine  = create_engine("postgresql://postgres:Dhimbulukweni1201@deloittecasestudy.c5saao6k8hrm.eu-north-1.rds.amazonaws.com:5432/postgres")

print("Works")

#Thi section is for loading the data into the tables

dim_location = clean_df[['neighbourhood_cleansed','latitude', 'longitude']].drop_duplicates()

dim_location.columns = ['neighbourhood_cleansed', 'latitude', 'longitude']
dim_location.to_sql("dim_location", engine, if_exists="append", index=False)

print("dim_loc saved")

dim_property = clean_df[['property_type', 'room_type', 'host_id', 'host_name']].drop_duplicates()
dim_property.to_sql('dim_property', engine, if_exists='append', index=False)
print("dim_property saved!")

# Load dim_weather
import datetime
weather_record_df = pd.DataFrame([{
'temperature': data["main"]["temp"],
'humidity': data["main"]["humidity"],
'wind_speed': data["wind"]["speed"],
'weather_description': data["weather"][0]["description"]
}])
weather_record_df.to_sql('dim_weather', engine, if_exists='append', index=False)
print("dim_weather saved!")

# Load dim_date
today = datetime.date.today()
dim_date_df = pd.DataFrame([{
'full_date': today,
'day': today.day,
'month': today.month,
'year': today.year,
'quarter': (today.month - 1) // 3 + 1
}])
dim_date_df.to_sql('dim_date', engine, if_exists='append', index=False)
print("dim_date saved!")

# Load fact_listings
fact_listings = clean_df[['id', 'name', 'price', 'minimum_nights', 'number_of_reviews', 'last_review', 'review_scores_rating', 'review_scores_location', 'availability_365']].copy()
fact_listings['location_key'] = 1
fact_listings['property_key'] = 1
fact_listings['weather_key'] = 1
fact_listings['date_key'] = 1
fact_listings.to_sql('fact_listings', engine, if_exists='append', index=False)
print("fact_listings saved!")
