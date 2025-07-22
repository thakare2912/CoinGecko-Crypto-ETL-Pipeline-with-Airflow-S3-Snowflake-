from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from airflow.models import Variable
import  pandas as pd
from datetime import datetime, timedelta
import requests
import json
import boto3


default_args = {
    'owner': 'thakare',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id="coingecko_data_pipeline",
    default_args=default_args,
    catchup=False
)

# Step 1: fetch data from CoinGecko API
def _fetch_data(**kwargs):
    all_coins = []
    page = 1

    while True:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': "usd",
            'order': 'market_cap_desc',
            'per_page': 250,
            'page': page,
            'sparkline': False
        }

        response = requests.get(url, params=params)
        data = response.json()

        if not data:
            break

        all_coins.extend(data) 
        page += 1

    filename = "coingecko_raw_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".json"

    kwargs['ti'].xcom_push(key='coingecko_filename', value=filename)
    kwargs['ti'].xcom_push(key='coingecko_data', value=json.dumps(all_coins))

    print(f"Fetched {len(all_coins)} coins.")
    print(f"Filename: {filename}")


#  4 read data from s3 
def _read_data_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_s3_airbnb")
    bucket_name = "spotify-etl-project-hanumant"
    prefix = "coin_data/raw_data/"

    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    coins_data = []

    for key in keys:
        if key.endswith(".json"):
            data = s3_hook.read_key(key, bucket_name=bucket_name)
            coins_data.append(json.loads(data))
    kwargs['ti'].xcom_push(key='s3_coins_data', value= coins_data)

#6  convert json data to csv
def _coin_data(**kwargs):
    coins_data = kwargs['ti'].xcom_pull(task_ids="read_data_from_s3", key='s3_coins_data')
    print(f"coins_data type: {type(coins_data)}, len: {len(coins_data)}")

    clean_coins = []
    for data in coins_data:
        if isinstance(data, list):
            clean_coins.extend([c for c in data if isinstance(c, dict) and 'name' in c])

    print(f"Total clean_coins: {len(clean_coins)}")

    coin_list = []
    for coin in clean_coins:
        coin_element = {
            'id': coin.get('id'),
            'symbol': coin.get('symbol'),
            'name': coin.get('name'),
            'current_price': coin.get('current_price'),
            'market_cap': coin.get('market_cap'),
            'market_cap_rank': coin.get('market_cap_rank'),
            'high_24h': coin.get('high_24h'),
            'low_24h': coin.get('low_24h'),
            'ath': coin.get('ath'),
            'atl': coin.get('atl'),
            'last_updated': coin.get('last_updated')
        }
        coin_list.append(coin_element)

    print(f"Total coin_list: {len(coin_list)}")

    coin_df = pd.DataFrame.from_dict(coin_list)
    coin_df = coin_df.drop_duplicates(subset='id')

    coin_buffer = StringIO()
    coin_df.to_csv(coin_buffer, index=False)
    coin_content = coin_buffer.getvalue()
    kwargs['ti'].xcom_push(key='coin_content', value=coin_content)

     
    


# Step 2: Create PythonOperator to run fetch_data
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=_fetch_data,
    dag=dag
)

# Step 3: Store data to S3 using S3CreateObjectOperator
store_data_to_s3 = S3CreateObjectOperator(
    task_id="store_data_to_s3",
    aws_conn_id="aws_s3_airbnb",  # Or your AWS connection ID
    s3_bucket="spotify-etl-project-hanumant",
    s3_key="coin_data/raw_data/{{ task_instance.xcom_pull(task_ids='fetch_data', key='coingecko_filename') }}",
    data="{{ task_instance.xcom_pull(task_ids='fetch_data', key='coingecko_data') }}",
    replace=True,
    dag=dag
)


# step 5: create task to read data from S3
read_data_from_s3 = PythonOperator(
    task_id = "read_data_from_s3",
    python_callable = _read_data_from_s3 ,
    provide_context=True,
        dag=dag,
)

# step7: create the task extract data from s3
coin_data = PythonOperator(
    task_id = 'extract_data_from_coin_data',
    python_callable = _coin_data,
    provide_context = True ,
    dag = dag
)

# step 8 : create the task load extract 
store_coin_to_s3 = S3CreateObjectOperator(
    task_id='store_coin_to_s3',
    aws_conn_id='aws_s3_airbnb',
    s3_bucket='spotify-etl-project-hanumant',
    s3_key='coin_data/coin_process data/coin_transformed_{{ ts_nodash }}.csv',
    data='{{ task_instance.xcom_pull(task_ids="extract_data_from_coin_data", key="coin_content") }}',
    replace=True,
    dag=dag,
)

fetch_data >> store_data_to_s3 >> read_data_from_s3 >> coin_data >> store_coin_to_s3
