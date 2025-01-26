
import time
import random

from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr


# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Connection name for MySQL
connection_name = "goit_mysql_db_maksymp"


def get_medal():
    medal_type = ['Bronze', 'Silver', 'Gold']
    medal = random.choice(medal_type)
    return medal

def check_medal(ti):
    medal = ti.xcom_pull(task_ids='get_medal')
    return 'calc_' + medal

def sleep_task(ti):
    time.sleep(5)

# Define the DAG
with DAG(
        'hw_7_maksymp',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["maksymp"]
) as dag:

    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS maksymp;
        """
    )

    create_table_task = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS maksymp.hw_7 (
        `id` int auto_increment primary key,
        `medal_type` varchar(10),
        `count` int,
        `created_at` datetime);
        """
    )

    get_medal_task = PythonOperator(
        task_id='get_medal',
        python_callable=get_medal,
    )

    check_medal_task = BranchPythonOperator(
        task_id='check_medal',
        python_callable=check_medal,
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO maksymp.hw_7 (medal_type, count, created_at)
            SELECT medal, count(*), now()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze'
            GROUP BY medal;
            """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO maksymp.hw_7 (medal_type, count, created_at)
            SELECT medal, count(*), now()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver'
            GROUP BY medal;
            """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO maksymp.hw_7 (medal_type, count, created_at)
            SELECT medal, count(*), now()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold'
            GROUP BY medal;
            """
    )

    sleep_task = PythonOperator(
        task_id='sleep_task',
        python_callable=sleep_task,
        trigger_rule=tr.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT timestampdiff(second, created_at, now()) < 30 FROM maksymp.hw_7
        ORDER BY id desc
        LIMIT 1;
        """,
        mode = 'poke',
        poke_interval=5,
        timeout = 31
    )

    # Define the tasks dependencies
    create_schema >> create_table_task >> get_medal_task >> check_medal_task
    check_medal_task >> [calc_Bronze, calc_Silver, calc_Gold] >> sleep_task >> check_for_correctness
