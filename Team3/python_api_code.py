import requests
import pandas

#Parameters that we are going to use for the request response
#Declaring  this at the top to avoid redundancy in code
city= 'Cape Town'
API = ''
url= 'https://api.openweathermap.org/data/2.5/weather'

#Parameters query location, application id and the units of measurement
parameters = {
    "q" : city,
    "appid" : API,
    "units" : "metric"
}

respon= requests.get(url, params=parameters)

weather_json = respon.json()

#This is the weather data that is being retrieved
weather_data= {
    'weather_condition' :weather_json['weather'][0]['main'],
    'Temperature' :weather_json['main']['temp'],
    'Pressure' :weather_json['main']['pressure'],
    'Humidity' :weather_json['main']['humidity'],
    'Clouds' :weather_json['main']['all'],
    'wind_speed' :weather_json['main']['speed']
    
}

#Loading the weather data in the pandas dataframe
weather_Repo = pandas.DataFrame([weather_data])

#Loading airbnb data into the url so that we can read it using the panda built in read method(function)
airbnb_url = 'http://data.insideairbnb.com/south-africa/wc/cape-town/2024-03-24/data/listings.csv.gz'

#Reading
airbnb_df = pandas.read_csv(airbnb_url)

#Ratings
collect_analysis = airbnb_df[[
    'Price',
    'NumberOfReviews',
    'ReviewRatingScores',
    'ReviewCleaninessScores',
    'ReviewLocatinScores'

]]

#Now we are adding the weather data
for col in weather_Repo.columns:
    collect_analysis[col] = weather_Repo.iloc[0][col]

correlate = collect_analysis.corr()

#printing the correlation 
print("The correlation between the weather and the airbnb ratings is")
print(correlate)

# Saving the dataset to a csv file
collect_analysis.to_csv("airbnb_weather_analysis.csv", index=False)
weather_Repo.to_csv("Weather_data.csv",index= False)