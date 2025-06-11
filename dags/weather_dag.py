from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


def extract_weather_data():
    http = HttpHook(method = 'GET', http_conn_id = 'weathermap_api')
    response = http.run(endpoint = '/data/2.5/weather?q=Chicago&APPID=aa4d27f3f6201f1b327515b01ee9e407')

    weather_data = json.loads(response.text)
    print(weather_data)
    return weather_data


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_weather_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    return df_data


def load_weather_data(task_instance):
    df_data = task_instance.xcom_pull(task_ids="transform_weather_data")
    csv_data = df_data.to_csv(index=False)
    now = datetime.now().strftime("%d%m%Y%H%M%S")
    file_name = f"current_weather_data_chicago_{now}.csv"


    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_string(
        string_data = csv_data,
        key = f"weather_data/{file_name}",
        bucket_name = "chicago-weather-data",
        replace = False
    )


with DAG('weather_dag',
        default_args=default_args,
        schedule = '@daily',
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Chicago&APPID=aa4d27f3f6201f1b327515b01ee9e407'
        )


        extract_weather_data = PythonOperator(
            task_id = "extract_weather_data",
            python_callable = extract_weather_data
        )


        transform_weather_data = PythonOperator(
            task_id = "transform_weather_data",
            python_callable = transform_weather_data
        )

        load_weather_data = PythonOperator(
            task_id = "load_weather_data",
            python_callable = load_weather_data
        )




is_weather_api_ready >> extract_weather_data >> transform_weather_data >> load_weather_data