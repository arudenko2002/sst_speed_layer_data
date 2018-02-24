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
from airflow.operators import SstLoadDataOperator, SstGcsExportOperator
from airflow.models import Variable

# setting start date to some value in the past
# we will not be using it, so it's only for consistency of
# DAG configuration
start_date = datetime(2017, 11, 15, 12, 30, 0, tzinfo=pytz.utc)

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
dag = DAG('metadata_mongo',
          description='Loads SST metadata to MongoDB',
          schedule_interval=None,
          default_args=default_args)

def mongo_conn():

    return "--username '{}' --password '{}' --authenticationDatabase '{}'".format(
      Variable.get('mongo_user'),
      Variable.get('mongo_password'),
      Variable.get('mongo_auth_db')
    )

def mongoimport_conn():
  return '{} --db {} --type json'.format(
      mongo_conn(),
      Variable.get('mongo_db')
    )

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

is_mongo_1_master = BashOperator(
    task_id = 'is_mongo_1_master',
    bash_command = 'mongo --host {}:{} {} --quiet --eval "d=db.isMaster(); print( d[\'ismaster\'] );"'.format(
        Variable.get('mongo_host_1'),
        Variable.get('mongo_port'),
        mongo_conn()),
    xcom_push = True,
    dag = dag
)

trigger_next = BashOperator(
        task_id = 'trigger_mongo_dev_sync',
        bash_command = 'airflow trigger_dag \'metadata_mongo_sync_dev\' -e \'{{ ds }}\'',
        dag = dag
)

ingest_tables = ['country', 'region', 'isrc', 'track', 'album', 'project', 'artist', 'partner', 'label_family', 'genre', 'day', 'week', 'sap_segment', 'partner_status']

for table in ingest_tables:

    create_bq_table = SstLoadDataOperator(
        task_id = 'bq_{}'.format(table),
        sql = 'sql/mongo_export_{}.sql'.format(table),
        destination_table = Variable.get('metadata_dst') + '.mongo_export_{}'.format(table),
        dag = dag
    )

    export_to_gcs = SstGcsExportOperator(
        task_id = 'gcs_{}'.format(table),
        source_table = Variable.get('metadata_dst') + '.mongo_export_{}'.format(table),
        gcs_uri = Variable.get('gcs_metadata_uri') + '/{}/{}.json'.format(table, table),
        dag = dag
    )

    download_to_fs = BashOperator(
        task_id = 'fs_{}'.format(table),
        bash_command='gsutil cp {}/{}/{}.json /tmp/{}.json'.format(
          Variable.get('gcs_metadata_uri'),
          table,
          table,
          table),
        dag = dag
    )

    mongo_drop_latest_collection = BashOperator(
        task_id = 'mongo_drop_{}_latest'.format(table),
        bash_command = 'mongo --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} {} --quiet --eval \'db.{}.drop()\''.format(
            Variable.get('mongo_port'),
            Variable.get('mongo_db'),
            mongo_conn(),
            '{}_latest'.format(table)),
        env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
            'HOST1': Variable.get('mongo_host_1'),
            'HOST2': Variable.get('mongo_host_2')},
        dag = dag
    )

    mongo_drop_old_collection = BashOperator(
        task_id = 'mongo_drop_{}_old'.format(table),
        bash_command = 'mongo --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} {} --quiet --eval \'db.{}.drop()\''.format(
            Variable.get('mongo_port'),
            Variable.get('mongo_db'),
            mongo_conn(),
            '{}_old'.format(table)),
        env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
            'HOST1': Variable.get('mongo_host_1'),
            'HOST2': Variable.get('mongo_host_2')},
        dag = dag
    )

    mongo_ingest_collection = BashOperator(
        task_id = 'mongo_ingest_{}_latest'.format(table),
        bash_command='mongoimport --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} --collection {} --file {}'.format(
          Variable.get('mongo_port'),
          mongoimport_conn(),
          '{}_latest'.format(table),
          '/tmp/{}.json'.format(table)),
        env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
            'HOST1': Variable.get('mongo_host_1'),
            'HOST2': Variable.get('mongo_host_2')},
        dag = dag
    )

    mongo_rename_old_collection = BashOperator(
        task_id = 'mongo_rename_{}'.format(table),
        bash_command = 'mongo --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} {} --quiet --eval \'db.{}.renameCollection("{}")\''.format(
            Variable.get('mongo_port'),
            Variable.get('mongo_db'),
            mongo_conn(),
            table,
            table + '_old'),
        env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
            'HOST1': Variable.get('mongo_host_1'),
            'HOST2': Variable.get('mongo_host_2'),
            'TS': '{{ ds_nodash }}' },
        dag = dag
    )

    mongo_rename_new_collection = BashOperator(
        task_id = 'mongo_rename_{}_latest'.format(table),
        bash_command = 'mongo --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} {} --quiet --eval \'db.{}.renameCollection("{}")\''.format(
            Variable.get('mongo_port'),
            Variable.get('mongo_db'),
            mongo_conn(),
            '{}_latest'.format(table),
            table),
        env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
            'HOST1': Variable.get('mongo_host_1'),
            'HOST2': Variable.get('mongo_host_2'),
            'TS': '{{ ds_nodash }}' },
        dag = dag
    )

    t_set_context >> create_bq_table >> export_to_gcs >> download_to_fs >> mongo_drop_old_collection >> mongo_drop_latest_collection >> mongo_ingest_collection
    is_mongo_1_master >> mongo_drop_old_collection 

    if table in ['artist', 'project', 'track']:
        mongo_index_collection = BashOperator(
            task_id = 'mongo_index_{}'.format(table),
            bash_command = 'mongo --host `if [ "$IS_HOST1_MASTER" == "true" ]; then echo $HOST1; else echo $HOST2; fi`:{} {} {} --quiet --eval \'db.{}.createIndex({{ {}: "text" }})\''.format(
                Variable.get('mongo_port'),
                Variable.get('mongo_db'),
                mongo_conn(),
                '{}_latest'.format(table),
                'name' if table == 'artist' else 'title'),
            env={'IS_HOST1_MASTER': '{{ ti.xcom_pull("is_mongo_1_master") }}',
                'HOST1': Variable.get('mongo_host_1'),
                'HOST2': Variable.get('mongo_host_2')},
            dag = dag
        )
        
        mongo_ingest_collection >> mongo_index_collection >> mongo_rename_old_collection >> mongo_rename_new_collection >> trigger_next

    else:

        mongo_ingest_collection >> mongo_rename_old_collection >> mongo_rename_new_collection >> trigger_next








