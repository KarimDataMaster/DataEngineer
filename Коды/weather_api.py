import datetime
import time
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import os

args = {
    'owner': 'dimon',
    'start_date':datetime.datetime(2018, 11, 1),
    'provide_context':True
}

#key_wwo = os.environ['KEY_API']
key_wwo = Variable.get("KEY_API_WWO")
print(key_wwo)
start_hour = 1
horizont_hours = 48

lat = 47.939
lng = 46.681
moscow_timezone = 3
local_timezone=4


def extract_data(**kwargs):
    ti = kwargs['ti']
    # Запрос на прогноз со следующего часа
    response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q':'{},{}'.format(lat,lng),
                'tp':'1',
                'num_of_days':2,
                'format':'json',
                'key':key_wwo
            },
            headers={
                'Authorization': key_wwo
            }
        )

    if response.status_code==200:
        # Разбор ответа сервера
        json_data = response.json()
        print(json_data)

        ti.xcom_push(key='weather_wwo_json', value=json_data)

def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='weather_wwo_json', task_ids=['extract_data'])[0]

    start_moscow = datetime.datetime.utcnow() + datetime.timedelta(hours=moscow_timezone)
    start_station = datetime.datetime.utcnow() + datetime.timedelta(hours=local_timezone)
    end_station = start_station + datetime.timedelta(hours=horizont_hours)

    date_list = []
    value_list = []
    weather_data = json_data['data']['weather']
    for weather_count in range(len(weather_data)):
        temp_date = weather_data[weather_count]['date']
        hourly_values = weather_data[weather_count]['hourly']
        for i in range(len(hourly_values)):
            date_time_str = '{} {:02d}:00:00'.format(temp_date, int(hourly_values[i]['time'])//100)
            date_list.append(date_time_str)
            value_list.append(hourly_values[i]['cloudcover'])

    res_df = pd.DataFrame(value_list,columns=['cloud_cover'])
    res_df['cloud_cover'] = res_df['cloud_cover'].astype('float')
    # Время предсказания (местное для рассматриваемой точки)
    res_df["date_to"] = date_list
    res_df["date_to"] = pd.to_datetime(res_df["date_to"])
    # Определение 48 интервала предсказания
    res_df = res_df[res_df['date_to'].between(start_station,end_station, inclusive=True)]
    # Время предсказания (по Москве)
    res_df["date_to"] = res_df["date_to"] + datetime.timedelta(hours=moscow_timezone - local_timezone)
    res_df["date_to"] = res_df["date_to"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # Время отправки запроса (по Москве)
    res_df["date_from"] = start_moscow
    res_df["date_from"] = pd.to_datetime(res_df["date_from"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Время получения ответа (по UTC)
    res_df["processing_date"] = res_df["date_from"]

    # print(res_df.head())
    # print(list(res_df.itertuples(index=False, name=None)))
    # print([x for x in res_df.iloc[0]])

    ti.xcom_push(key='weather_wwo_df', value=res_df)

def load_data(**kwargs):
    ti = kwargs['ti']
    res_df = ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0]
    print(res_df.head())
    print([x for x in res_df.iloc[0]])

                    

with DAG('load_weather_wwo', description='load_weather_wwo', schedule_interval='*/1 * * * *',  catchup=False,default_args=args) as dag: #0 * * * *   */1 * * * *
        extract_data    = PythonOperator(task_id='extract_data', python_callable=extract_data)
        transform_data  = PythonOperator(task_id='transform_data', python_callable=transform_data)
        #load_data       = PythonOperator(task_id='load_data', python_callable=load_data)
        create_cloud_table = PostgresOperator(
                                task_id="create_clouds_value_table",
                                postgres_conn_id="database_PG",
                                sql="""
                                    CREATE TABLE IF NOT EXISTS clouds_value (
                                    clouds FLOAT NOT NULL,
                                    date_to TIMESTAMP NOT NULL,
                                    date_from TIMESTAMP NOT NULL,
                                    processing_date TIMESTAMP NOT NULL);
                                """,
                                )
        insert_in_table = PostgresOperator(
                                task_id="insert_clouds_table",
                                postgres_conn_id="database_PG",
                                sql=[f"""INSERT INTO clouds_value VALUES(
                                 {{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['cloud_cover']}}}},
                                '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['date_to']}}}}',
                                '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['date_from']}}}}',
                                '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['processing_date']}}}}')
                                """ for i in range(5)]
                                )

        extract_data >> transform_data >> create_cloud_table >> insert_in_table

# Добавить в docker-compose

    # database:
    #     image: "postgres" # use latest official postgres version
    #     env_file:
    #         - database.env # configure postgres
    #     ports:
    #         - "5423:5432"

#  database.env
# POSTGRES_USER=unicorn_user
# POSTGRES_PASSWORD=magical_password
# POSTGRES_DB=rainbow_database