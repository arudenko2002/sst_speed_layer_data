from __future__ import print_function
import airflow
import logging
import sys
import pytz

from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import SstLoadDataOperator, SstLastUpdateOperator, SstQueryDataOperator, SstIncrementalLoadDataOperator, SstPeriodStackOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

# setting start date to some value in the past
# we will not be using it, so it's only for consistency of
# DAG configuration
start_date = datetime(2017, 11, 15, 19, 30, 0, tzinfo=pytz.utc)

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
dag = DAG('transactions_staging_v3',
          description='Loads daily SST transactions into staging',
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

last_update_timestamp = SstLastUpdateOperator(
    task_id = 'last_update_timestamp',
    dataset_table = Variable.get('transactions_dst'),
    dag = dag

)

incremental_load = SstIncrementalLoadDataOperator(
    task_id = 'incremental_load',
    sql = 'sql/transactions_daily_partition.sql',
    partition_list_sql = 'sql/transactions_partition_list.sql',
    source_table = '{{ var.value.transactions_src }}',
    destination_table = '{{ var.value.transactions_dst }}',
    source_partition_column = 'transaction_date',
    destination_partition_column = 'partner_report_date',
    last_update_column = 'load_datetime', 
    last_update_value = '{{ ti.xcom_pull("last_update_timestamp") }}',
    execution_time = '{{ ts }}',
    dag = dag
)

update_period_stack = SstPeriodStackOperator(
    task_id = 'update_period_stack',
    dates = '{{ ti.xcom_pull("incremental_load") }}',
    max_daily_periods_processed = 15, 
    max_weekly_periods_processed = 45,
    calendar_metadata_table = '{{ var.value.metadata_src }}.day',
    destination_table = '{{ var.value.period_stack_table }}',
    dag = dag

)


trigger_next = BashOperator(
        task_id = 'trigger_ssd_speed_layer_changeset_daily',
        bash_command = 'airflow trigger_dag \'ssd-speed-layer-changeset-daily\' -e \'{{ ds }}\'',
        dag = dag
)



t_set_context >> last_update_timestamp >> incremental_load >> update_period_stack >> trigger_next







