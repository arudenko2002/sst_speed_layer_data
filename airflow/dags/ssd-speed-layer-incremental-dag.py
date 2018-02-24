#from __future__ import print_function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.sensors import HttpSensor
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators import SstQueryDataOperator

from datetime import datetime, timedelta
import pytz

#start_date = datetime(2017, 07, 05, 13, 0, 0, tzinfo=pytz.utc);


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['applicationsupport@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}



#stage names
stage_build = 'BUILD'
stage_load = 'LOAD'

#BT table names
bt_table_name_artist = '{{var.value.sst_gcp_bt_table_name_artist}}'
bt_table_name_track = '{{var.value.sst_gcp_bt_table_name_track}}'
bt_table_name_project = '{{var.value.sst_gcp_bt_table_name_project}}'
bt_table_name_isrc = '{{var.value.sst_gcp_bt_table_name_isrc}}'
bt_table_name_album = '{{var.value.sst_gcp_bt_table_name_album}}'

bt_table_name_top_artist = '{{var.value.sst_gcp_bt_table_name_topartist}}'
bt_table_name_top_track = '{{var.value.sst_gcp_bt_table_name_toptrack}}'
bt_table_name_top_project = '{{var.value.sst_gcp_bt_table_name_topproject}}'

query_check_stack_dates = 'select count(*) as count_records from `{{ var.value.metadata_dst }}.period_process_stack` where status = \'scheduled\''

query_remove_stack_dates = 'delete from `{{ var.value.metadata_dst }}.period_process_stack` where status = \'scheduled\''

#BT cluster size
bt_max_cluster_node_size = '{{var.value.sst_gcp_bt_max_cluster_nodes}}'
bt_min_cluster_node_size = '{{var.value.sst_gcp_bt_min_cluster_nodes}}'

#df worker node size
df_max_worker_artist = '40'
df_max_worker_project = '50'
df_max_worker_track = '90'
df_max_worker_album = '40'
df_max_worker_isrc = '70'
df_max_worker_top_artist = '40'
df_max_worker_top_project = '70'
df_max_worker_top_track = '70'
df_startup_worker_node = '10'

sst_job_start_date = '{{ macros.ds_format(macros.ds_add(ds, -0), "%Y-%m-%d" , "%Y%m%d" ) }}'
sst_job_end_date = '{{ macros.ds_format(macros.ds_add(ds, -0), "%Y-%m-%d" , "%Y%m%d" ) }}'
#sst_job_start_date = '20171004'
#sst_job_end_date = '20171007'

sst_job_product_date = '{{ macros.ds_add(ds, -0) }}'
#sst_job_product_date = '{{ yesterday_ds }}'
#sst_job_load_date = '{{ ds }}'
sst_job_load_date = '{{ macros.ds_add(ds, -0) }}'

def createDag(dagName):
    return DAG(
        dag_id=dagName,
        schedule_interval=None,
        #schedule_interval='30,20,*,*,*',
        start_date=datetime(2017, 11, 15),
        default_args=default_args
    )

def changeBTNodeCount(task_name, node_count, dagObj):
    return BashOperator(
        task_id=task_name,
        bash_command = 'gcloud beta bigtable clusters update {{ var.value.gcp_bt_cluster_id }} --instance={{ var.value.sst_gcp_bt_instance_id }} --num-nodes=' + node_count,
        dag=dagObj

    )

def increaseBTClusterSize(dagObj):
    return changeBTNodeCount('increase-bt-node-size', bt_max_cluster_node_size, dagObj)

def decreaseBTClusterSize(dagObj):
    return changeBTNodeCount('decrease-bt-node-size', bt_min_cluster_node_size, dagObj)



def createBashOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, jobLoadDate, dagObj, deltaAggMode):
    if startShard == "" or endShard == "":
        bashcommand = 'java -jar {{ var.value.sst_jar_base_incremental }}{{ var.value.sst_jar_name }} --module=' + module + \
                       ' --stage=' + stage + \
                       ' --startDate=' + startDate + \
                       ' --endDate=' + endDate + \
                       ' --loadDate=' + jobLoadDate + \
                       ' --productDate=' + sst_job_product_date + \
                       ' --project={{ var.value.gcp_project_id }}' \
                       ' --dynamicDateMode=Y' \
                       ' --deltaAggMode=' + deltaAggMode
    else:
        bashcommand = 'java -jar {{ var.value.sst_jar_base_incremental }}{{ var.value.sst_jar_name }} --module=' + module + \
                      ' --stage=' + stage + \
                      ' --startShard=' + startShard + \
                      ' --endShard=' + endShard + \
                      ' --startDate=' + startDate + \
                      ' --endDate=' + endDate + \
                      ' --loadDate=' + jobLoadDate + \
                      ' --productDate=' + sst_job_product_date + \
                      ' --project={{ var.value.gcp_project_id }}' \
                      ' --dynamicDateMode=Y' \
                      ' --deltaAggMode=' + deltaAggMode

    return BashOperator(
        task_id=task_name,
        bash_command = bashcommand,
        dag=dagObj

)

def createDataflowOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, bigTableName, dagObj, df_max_worker):
    return DataFlowJavaOperator(
        task_id=task_name,
        jar='{{ var.value.sst_jar_base_incremental }}{{ var.value.sst_jar_name }}',
        options={
            'project': '{{ var.value.gcp_project_id }}',
            'zone': '{{ var.value.gcp_df_zone }}',
            'stagingLocation': '{{ var.value.staging_location }}',
            'autoscalingAlgorithm': 'THROUGHPUT_BASED',
            'workerMachineType': '{{ var.value.sst_gcp_df_machine_type }}',
            'bigtableProjectId': '{{ var.value.sst_gcp_bt_project_id }}',
            'bigtableInstanceId': '{{ var.value.sst_gcp_bt_instance_id }}',
            'bigtableTableId': bigTableName,
            'numWorkers': df_startup_worker_node,
            'defaultWorkerLogLevel': 'ERROR',
            'workerLogLevelOverrides': '{"com.umusic.gcp.sst.speedlayer.data":"ERROR"}',
            'module': module,
            'stage': stage,
            'startShard': startShard,
            'endShard': endShard,
            'startDate': startDate,
            'endDate': endDate,
            'loadDate': sst_job_load_date,
            'productDate' : sst_job_product_date,
            'maxNumWorkers': df_max_worker,
            'dynamicDateMode': 'Y'
        },
        dag=dagObj
)



def checkAvailability(*args, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key=None, task_ids='query_stack_dates')
    #result = '{{ ti.xcom_pull(task_ids="query-data") }}'
    countSt = result[0]['count_records']
    print countSt
    if(countSt > 0):
        return True
    else:
        return False


# All Dag & Task declartion follows

# dag & task
#create new dag
incDag = createDag('ssd-speed-layer-incremental')

#task to query period stack dates in period stack table
t_query_stack_dates = SstQueryDataOperator(
    task_id = 'query_stack_dates',
    sql = query_check_stack_dates,
    dag = incDag

)


#task to check period stack date availability
t_check_stack_dates_availability = ShortCircuitOperator(
    task_id='check_stack_dates',
    python_callable=checkAvailability,
    provide_context=True,
    depends_on_past=True,
    dag=incDag
)

#task to remove period stack dates in period stack table
t_remove_processed_stack_dates = SstQueryDataOperator(
    task_id = 'remove_processed_stack_dates',
    sql = query_remove_stack_dates,
    dag = incDag

)

trigger_next = BashOperator(
        task_id = 'trigger_metadata_mongo',
        bash_command = 'airflow trigger_dag \'metadata_mongo\' -e \'{{ ds }}\'',
        dag = incDag
)


#change BT Node size task
t_increase_bt_node_size = increaseBTClusterSize(incDag)

t_decrease_bt_node_size = decreaseBTClusterSize(incDag)

#artist dag task
t_build_aggregate_artist = createBashOprTask('build-aggregate-artist', 'ARTIST', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'Y')

t_load_aggregate_artist = createDataflowOprTask('load-aggregate-artist', 'ARTIST', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date, bt_table_name_artist, incDag, df_max_worker_artist)
#15000211

#project  task
t_build_aggregate_project = createBashOprTask('build-aggregate-project', 'PROJECT', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'Y')

t_load_aggregate_project = createDataflowOprTask('load-aggregate-project', 'PROJECT', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date, bt_table_name_project, incDag, df_max_worker_project)
#15000211


#track  task
t_build_aggregate_track = createBashOprTask('build-aggregate-track', 'TRACK', stage_build, '15000101', '15000703', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'Y')

t_load_aggregate_track = createDataflowOprTask('load-aggregate-track', 'TRACK', stage_load, '15000101', '15000703', sst_job_start_date, sst_job_end_date, bt_table_name_track, incDag, df_max_worker_track)
#15000703


#isrc  task
t_build_aggregate_isrc = createBashOprTask('build-aggregate-isrc', 'ISRC', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'Y')

t_load_aggregate_isrc = createDataflowOprTask('load-aggregate-isrc', 'ISRC', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date, bt_table_name_isrc, incDag, df_max_worker_isrc)
#15000211



#aggreagte and load task
t_build_aggregate_album = createBashOprTask('build-aggregate-album', 'ALBUM', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'Y')

t_load_aggregate_album = createDataflowOprTask('load-aggregate-album', 'ALBUM', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date, bt_table_name_album, incDag, df_max_worker_album)
#15000211



#aggreagte and load task
t_build_aggregate_top_artist = createBashOprTask('build-aggregate-top-artist', 'TOPARTIST', stage_build, '15000101', '15000421', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'N')

t_load_aggregate_top_artist = createDataflowOprTask('load-aggregate-top-artist', 'TOPARTIST', stage_load, '15000101', '15000421', sst_job_start_date, sst_job_end_date, bt_table_name_top_artist, incDag, df_max_worker_top_artist)
#15000421




#aggreagte and load task
t_build_aggregate_top_project = createBashOprTask('build-aggregate-top-project', 'TOPPROJECT', stage_build, '15000101', '15001221', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'N')

#15001221
t_load_aggregate_top_project = createDataflowOprTask('load-aggregate-top-project', 'TOPPROJECT', stage_load, '15000101', '15001221', sst_job_start_date, sst_job_end_date, bt_table_name_top_project, incDag, df_max_worker_top_project)



#aggreagte and load task
t_build_aggregate_top_track = createBashOprTask('build-aggregate-top-track', 'TOPTRACK', stage_build, '15000101', '15001221', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'N')

#15001221
t_load_aggregate_top_track = createDataflowOprTask('load-aggregate-top-track', 'TOPTRACK', stage_load, '15000101', '15001221', sst_job_start_date, sst_job_end_date, bt_table_name_top_track, incDag, df_max_worker_top_track)


#t_remove_process_stack_dates = createBashOprTask('remove-process-stack-dates', 'TOPTRACK', 'REMOVEPROCESSEDDATE', '', '', sst_job_start_date, sst_job_end_date, sst_job_load_date, incDag, 'N');
#connect tasks pipeline

t_query_stack_dates.set_downstream(t_check_stack_dates_availability)

t_check_stack_dates_availability.set_downstream(t_build_aggregate_artist)

t_check_stack_dates_availability.set_downstream(t_build_aggregate_project)

t_check_stack_dates_availability.set_downstream(t_build_aggregate_track)

t_check_stack_dates_availability.set_downstream(t_build_aggregate_album)

t_check_stack_dates_availability.set_downstream(t_build_aggregate_isrc)



t_build_aggregate_artist >> t_build_aggregate_top_artist >> t_increase_bt_node_size
t_build_aggregate_project >> t_build_aggregate_top_project >> t_increase_bt_node_size
t_build_aggregate_track >> t_build_aggregate_top_track >> t_increase_bt_node_size
t_build_aggregate_album >> t_increase_bt_node_size
t_build_aggregate_isrc >> t_increase_bt_node_size

#t_build_aggregate_top_artist >> t_increase_bt_node_size
#t_build_aggregate_top_track >> t_increase_bt_node_size
#t_build_aggregate_top_project >> t_increase_bt_node_size

'''
t_increase_bt_node_size >> t_load_aggregate_artist
t_increase_bt_node_size >> t_load_aggregate_project

t_load_aggregate_artist >> t_load_aggregate_track

t_load_aggregate_project >> t_load_aggregate_album

t_load_aggregate_track >> t_load_aggregate_isrc

t_load_aggregate_album >> t_load_aggregate_top_artist

t_load_aggregate_isrc >> t_load_aggregate_top_project


t_load_aggregate_top_artist >> t_load_aggregate_top_track

t_load_aggregate_top_track >> t_decrease_bt_node_size

t_load_aggregate_top_project >> t_decrease_bt_node_size
'''

#t_increase_bt_node_size >> t_load_aggregate_artist >> t_load_aggregate_project >> t_load_aggregate_track >> t_load_aggregate_album >> t_load_aggregate_isrc >> t_load_aggregate_top_artist >> t_load_aggregate_top_project >> t_load_aggregate_top_track >> t_decrease_bt_node_size


t_increase_bt_node_size >> t_load_aggregate_artist

t_increase_bt_node_size >> t_load_aggregate_project

t_increase_bt_node_size >> t_load_aggregate_track

t_increase_bt_node_size >> t_load_aggregate_album

t_increase_bt_node_size >> t_load_aggregate_isrc

t_increase_bt_node_size >> t_load_aggregate_top_artist

t_increase_bt_node_size >> t_load_aggregate_top_project

t_increase_bt_node_size >> t_load_aggregate_top_track


t_load_aggregate_artist >> t_decrease_bt_node_size

t_load_aggregate_project >> t_decrease_bt_node_size

t_load_aggregate_track >> t_decrease_bt_node_size

t_load_aggregate_album >> t_decrease_bt_node_size

t_load_aggregate_isrc >> t_decrease_bt_node_size

t_load_aggregate_top_artist >> t_decrease_bt_node_size

t_load_aggregate_top_project >> t_decrease_bt_node_size

t_load_aggregate_top_track >> t_decrease_bt_node_size


t_decrease_bt_node_size >> t_remove_processed_stack_dates >> trigger_next