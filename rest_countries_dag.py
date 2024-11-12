from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

from typing import Dict, List
import psycopg2
import requests
import logging


def get_Redshift_connection(autocommit:bool=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url:str) -> List[Dict]:
    logging.info(datetime.utcnow())
    api_data = requests.get(url)
    return api_data.json()


@task
def transform(dict_data:List[Dict]) -> List[List[str]]:
    records = []
    for x in dict_data:
        country = x["name"]["official"]
        population = x["population"]
        area = x["area"]
        records.append([country, population, area])
    logging.info("Transform ended")
    return records


@task
def load(schema:str, table:str, records:List[List[str]]):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    """
    records = [
        [Gibraltar, 33691, 6.0]],
        [State of Palestine, 4803269, 6220.0]
        ...
    ]
    """
    
    try:
        cur.execute("BEGIN;")
        # FULL REFRESH
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        cur.execute(f"CREATE TABLE {schema}.{table} \
                    (country varchar(128), \
                    population int, \
                    area float)")
        for r in records:
            country = r[0]
            population = r[1]
            area = r[2]
            print(country, "-", population, "-", area)
            cur.execute("""
                        INSERT INTO {}.{} (country, population, area) VALUES (%s, %s, %s)
                        """.format(schema, table),
                        (country, population, area))
        cur.execute("COMMIT;")   # cur.execute("END;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
    logging.info("load done")


with DAG(
    dag_id='rest_countries',
    start_date=datetime(2024, 11, 11),
    schedule='30 6 * * 6',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = Variable.get("csv_url")
    schema = 'kudosjdm'
    table = 'country_info'

    lines = transform(extract(url))
    load(schema, table, lines)