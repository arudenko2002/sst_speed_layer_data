from __future__ import print_function

import logging
import sys
import pytz

from os import path

from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import SstLoadDataOperator
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# setting start date to some value in the past
# we will not be using it, so it's only for consistency of
# DAG configuration
start_date = datetime(2017, 11, 30, 7, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None,
    'email': ['dmitri.safin@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)

}


# declare DAG
dag = DAG('metadata_staging',
          description='Reloads SST metadata in staging',
          schedule_interval=None,
          default_args=default_args)


def set_context(**kwargs):

    
    # set current week end date
    execution_date = kwargs['ds']
    dt = datetime.strptime(execution_date, '%Y-%m-%d')
    start_of_week = dt - timedelta(days=dt.weekday())
    end_of_week = start_of_week + timedelta(days=5)



    return {
        'end_of_week': end_of_week.strftime('%Y-%m-%d')
    }

t_set_context = PythonOperator(
    task_id='set_context',
    python_callable=set_context,
    provide_context=True,
    retries=0,
    dag=dag
)

product_shard = SstLoadDataOperator(
    task_id = 'product_shard',
    sql = 'sql/product_daily_shard.sql',
    destination_table = Variable.get('metadata_dst') + '.product',
    partition = '{{ ds }}',
    shard = True,
    dag = dag
)

product_change_history = SstLoadDataOperator(
    task_id = 'product_change_history',
    sql = 'sql/product_change_history.sql',
    destination_table = Variable.get('metadata_dst') + '.product_change_history',
    partition = '{{ ds }}',
    dag = dag
)

changeset_artist_daily = SstLoadDataOperator(
    task_id = 'changeset_artist_daily',
    sql = 'sql/changeset_artist_daily.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_artist_daily',
    partition = '{{ ds }}',
    dag = dag
)


changeset_artist_weekly = SstLoadDataOperator(
    task_id = 'changeset_artist_weekly',
    sql = 'sql/changeset_artist_weekly.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_artist_weekly',
    partition = '{{ ti.xcom_pull("set_context")["end_of_week"] }}',
    write_disposition = 'WRITE_APPEND',
    dag = dag
)


changeset_project_daily = SstLoadDataOperator(
    task_id = 'changeset_project_daily',
    sql = 'sql/changeset_project_daily.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_project_daily',
    partition = '{{ ds_nodash }}',
    dag = dag
)

changeset_project_weekly = SstLoadDataOperator(
    task_id = 'changeset_project_weekly',
    sql = 'sql/changeset_project_weekly.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_project_weekly',
    partition = '{{ ti.xcom_pull("set_context")["end_of_week"] }}',
    write_disposition='WRITE_APPEND',
    dag = dag
)


changeset_track_daily = SstLoadDataOperator(
    task_id = 'changeset_track_daily',
    sql = 'sql/changeset_track_daily.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_track_daily',
    partition = '{{ ds}}',
    dag = dag
)

changeset_track_weekly = SstLoadDataOperator(
    task_id = 'changeset_track_weekly',
    sql = 'sql/changeset_track_weekly.sql',
    destination_table = Variable.get('metadata_dst') + '.changeset_track_weekly',
    partition = '{{ ti.xcom_pull("set_context")["end_of_week"] }}',
    write_disposition='WRITE_APPEND',
    dag = dag
)



load_account = SstLoadDataOperator(
    task_id = 'load_account',
    sql = 'sql/load_account.sql',
    destination_table = Variable.get('metadata_dst') + '.account',
    dag = dag
)

load_country = SstLoadDataOperator(
    task_id = 'load_country',
    sql = 'sql/load_country.sql',
    destination_table = Variable.get('metadata_dst') + '.country',
    dag = dag
)

load_currency = SstLoadDataOperator(
    task_id = 'load_currency',
    sql = 'sql/load_currency.sql',
    destination_table = Variable.get('metadata_dst') + '.currency',
    dag = dag
)

load_day = SstLoadDataOperator(
    task_id = 'load_day',
    sql = 'sql/load_day.sql',
    destination_table = Variable.get('metadata_dst') + '.day',
    dag = dag
)

load_local_product = SstLoadDataOperator(
    task_id = 'load_local_product',
    sql = 'sql/load_local_product.sql',
    destination_table = Variable.get('metadata_dst') + '.local_product',
    dag = dag
)

load_master_account = SstLoadDataOperator(
    task_id = 'load_master_account',
    sql = 'sql/load_master_account.sql',
    destination_table = Variable.get('metadata_dst') + '.master_account',
    dag = dag
)

load_ratio = SstLoadDataOperator(
    task_id = 'load_ratio',
    sql = 'sql/load_ratio.sql',
    destination_table = Variable.get('metadata_dst') + '.ratio',
    dag = dag
)

load_region = SstLoadDataOperator(
    task_id = 'load_region',
    sql = 'sql/load_region.sql',
    destination_table = Variable.get('metadata_dst') + '.region',
    dag = dag
)

load_usage = SstLoadDataOperator(
    task_id = 'load_usage',
    sql = 'sql/load_usage.sql',
    destination_table = Variable.get('metadata_dst') + '.usage',
    dag = dag
)

build_project_genre = SstLoadDataOperator(
    task_id = 'build_project_genre',
    sql = 'sql/build_project_genre.sql',
    destination_table = Variable.get('metadata_dst') + '.project_genre',
    dag = dag
)

build_track_genre = SstLoadDataOperator(
    task_id = 'build_track_genre',
    sql = 'sql/build_track_genre.sql',
    destination_table = Variable.get('metadata_dst') + '.track_genre',
    dag = dag
)

trigger_next = BashOperator(
    task_id = 'trigger_transactions_staging_v3',
    bash_command = 'airflow trigger_dag \'transactions_staging_v3\' -e \'{{ ds }}\'',
    dag = dag
)



t_set_context >> load_account >> trigger_next
t_set_context >> load_country >> trigger_next
t_set_context >> load_currency >> trigger_next
t_set_context >> load_day >> trigger_next
t_set_context >> load_local_product >> trigger_next
t_set_context >> load_master_account >> trigger_next
t_set_context >> load_ratio >> trigger_next
t_set_context >> load_region >> trigger_next
t_set_context >> load_usage >> trigger_next

t_set_context >> product_shard

product_shard >> build_project_genre >> trigger_next

product_shard >> build_track_genre >> trigger_next

product_shard >> product_change_history >> trigger_next
product_shard >> changeset_artist_daily >> trigger_next
product_shard >> changeset_artist_weekly >> trigger_next
product_shard >> changeset_project_daily >> trigger_next
product_shard >> changeset_project_weekly >> trigger_next
product_shard >> changeset_track_daily >> trigger_next
product_shard >> changeset_track_weekly >> trigger_next







