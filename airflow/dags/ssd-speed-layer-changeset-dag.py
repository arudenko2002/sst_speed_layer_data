#from __future__ import print_function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.sensors import HttpSensor
from airflow.operators import SstQueryDataOperator

from datetime import datetime, timedelta
import pytz

#start_date = datetime(2017, 07, 05, 13, 0, 0, tzinfo=pytz.utc);


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['dmitri.safin@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'catchup': False,
    'retry_delay': timedelta(minutes=2)

}



#stage names
stage_build = 'CHANGESETBUILD'
stage_load = 'CHANGESETLOAD'
stage_unload = 'CHANGESETUNLOAD'




#BT table names
bt_table_name_artist = '{{var.value.sst_gcp_bt_table_name_artist}}'
bt_table_name_track = '{{var.value.sst_gcp_bt_table_name_track}}'
bt_table_name_project = '{{var.value.sst_gcp_bt_table_name_project}}'
bt_table_name_isrc = '{{var.value.sst_gcp_bt_table_name_isrc}}'
bt_table_name_album = '{{var.value.sst_gcp_bt_table_name_album}}'




#BT cluster size
bt_max_cluster_node_size = '100'
bt_min_cluster_node_size = '4'


df_max_worker_artist = '50'
df_max_worker_project = '60'
df_max_worker_track = '70'
df_max_worker_album = '30'

df_startup_worker_node = '10'
sst_job_start_date = '{{ macros.ds_format(macros.ds_add(ds, -28), "%Y-%m-%d" , "%Y%m%d" ) }}'
sst_job_end_date = '{{ macros.ds_format(macros.ds_add(ds, -0), "%Y-%m-%d" , "%Y%m%d" ) }}'
#sst_job_start_date = '20170801'
#sst_job_end_date = '20171004'
sst_chngst_start_date = '{{ macros.ds_add(ds, -0) }}'
sst_chngst_end_date = '{{ macros.ds_add(ds, -0) }}'
#sst_chngst_start_date = '2017-10-01'
#sst_chngst_end_date = '2017-10-20'
#sst_job_product_date = '{{ yesterday_ds }}'
sst_job_product_date = '{{ macros.ds_add(ds, -0) }}'

sst_job_load_date = '{{ ds }}'


#queries to check changeset
query_artist_changeset = 'select count(*) as count_records from `consumption_speedlayer_staging.changeset_artist_daily` ch where ch._partitiontime >= timestamp(\'{}\') and ch._partitiontime <= timestamp(\'{}\')'
query_project_changeset = 'select count(*) as count_records from `consumption_speedlayer_staging.changeset_project_daily` ch where ch._partitiontime >= timestamp(\'{}\') and ch._partitiontime <= timestamp(\'{}\')'
query_track_changeset = 'select count(*) as count_records from `consumption_speedlayer_staging.changeset_track_daily` ch where ch._partitiontime >= timestamp(\'{}\') and ch._partitiontime <= timestamp(\'{}\')'

def createDag(dagName):
    return DAG(
        dag_id=dagName,
       # schedule_interval='30,20,*,*,*',
        schedule_interval=None,
        start_date=datetime(2017, 11, 15),
        default_args=default_args
    )

def changeBTNodeCount(task_name, node_count, dagObj):
    return BashOperator(
        task_id=task_name,
        bash_command = 'gcloud beta bigtable clusters update {{ var.value.gcp_bt_cluster_id }} --instance={{ var.value.sst_gcp_bt_instance_id }} --num-nodes=' + node_count,
        dag=dagObj

    )

def increaseBTClusterSize(dagObj, name):
    return changeBTNodeCount('increase-bt-node-size-' + name, bt_max_cluster_node_size, dagObj)

def decreaseBTClusterSize(dagObj, name):
    return changeBTNodeCount('decrease-bt-node-size-' + name, bt_min_cluster_node_size, dagObj)

def createBashOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, chngstStartDate, chngstEndDate, changesetType, jobLoadDate, dagObj):
    if startShard == "" or endShard == "":
        bashcommand = 'java -jar {{ var.value.sst_jar_base_incremental }}{{ var.value.sst_jar_name }} --module=' + module + \
                       ' --stage=' + stage + \
                       ' --startDate=' + startDate + \
                       ' --endDate=' + endDate + \
                       ' --loadDate=' + jobLoadDate + \
                       ' --productDate=' + sst_job_product_date + \
                       ' --changesetStartDate=' + chngstStartDate + \
                       ' --changesetEndDate=' + chngstEndDate + \
                       ' --changeset=' + changesetType + \
                       ' --project={{ var.value.gcp_project_id }}'
    else:
        bashcommand = 'java -jar {{ var.value.sst_jar_base_incremental }}{{ var.value.sst_jar_name }} --module=' + module + \
                      ' --stage=' + stage + \
                      ' --startShard=' + startShard + \
                      ' --endShard=' + endShard + \
                      ' --startDate=' + startDate + \
                      ' --endDate=' + endDate + \
                      ' --loadDate=' + jobLoadDate + \
                      ' --productDate=' + sst_job_product_date + \
                      ' --changesetStartDate=' + chngstStartDate + \
                      ' --changesetEndDate=' + chngstEndDate + \
                      ' --changeset=' + changesetType + \
                      ' --project={{ var.value.gcp_project_id }}'

    return BashOperator(
        task_id=task_name,
        bash_command = bashcommand,
        dag=dagObj

    )

def createDataflowOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, changesetType, bigTableName, dagObj, df_max_worker):
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
            'changesetStartDate': sst_chngst_start_date,
            'changesetEndDate': sst_chngst_end_date,
            'changeset': changesetType,
            'maxNumWorkers': df_max_worker
        },
        dag=dagObj
    )


def checkAvailability(*args, **kwargs):

    queryTaskId = kwargs['quertTaskId']
    print 'queryTaskId is ' + queryTaskId
    ti = kwargs['ti']
    result = ti.xcom_pull(key=None, task_ids=queryTaskId)
    countSt = result[0]['count_records']
    print countSt
    if(countSt > 0):
        return True
    else:
        return False

def createQueryDataOprTask(taskId, dagObj, sql):
    sql = sql.format(sst_chngst_start_date, sst_chngst_end_date)
    print 'sql is' + sql
    return SstQueryDataOperator(
    task_id = taskId,
    sql = sql,
    dag = dagObj

)


def createShortCircuitOprTask(shrtCirtaskId, dagObj, queryTaskId):
    return ShortCircuitOperator(
    task_id=shrtCirtaskId,
    python_callable=checkAvailability,
    provide_context=True,
    depends_on_past=True,
    dag=dagObj,
    op_kwargs=[("quertTaskId", queryTaskId)]
)



# All Dag & Task declartion follows


# dag & task
#create new dag
chngDailyDag = createDag('ssd-speed-layer-changeset-daily')

trigger_next = BashOperator(
        task_id = 'trigger_ssd_speed_layer_incremental',
        bash_command = 'airflow trigger_dag \'ssd-speed-layer-incremental\' -e \'{{ ds }}\'',
        dag = chngDailyDag
)


t_query_artist_chngset = createQueryDataOprTask('query-chngset-artist', chngDailyDag, query_artist_changeset)

t_query_project_chngset = createQueryDataOprTask('query-chngset-project', chngDailyDag, query_project_changeset)

t_query_track_chngset = createQueryDataOprTask('query-chngset-track', chngDailyDag, query_track_changeset)



t_check_artist_chngset = createShortCircuitOprTask('check-chngset-artist', chngDailyDag, 'query-chngset-artist')

t_check_project_chngset = createShortCircuitOprTask('check-chngset-project', chngDailyDag, 'query-chngset-project')

t_check_track_chngset = createShortCircuitOprTask('check-chngset-track', chngDailyDag, 'query-chngset-track')


#change BT Node size task
t_increase_bt_node_size_unload = increaseBTClusterSize(chngDailyDag, 'unload')

t_decrease_bt_node_size_unload = decreaseBTClusterSize(chngDailyDag, 'unload')

#artist dag task
t_build_chngset_aggregate_artist = createBashOprTask('build-chngset-aggregate-artist', 'ARTIST', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_chngst_start_date, sst_chngst_end_date, 'ARTIST_DAILY', sst_job_load_date, chngDailyDag)

t_load_chngset_aggregate_artist = createDataflowOprTask('load-chngset-aggregate-artist', 'ARTIST', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date,  'ARTIST_DAILY', bt_table_name_artist,  chngDailyDag, df_max_worker_artist)

t_unload_chngset_aggregate_artist = createDataflowOprTask('unload-chngset-aggregate-artist', 'ARTIST', stage_unload, '15000101', '15000211', sst_job_start_date, sst_job_end_date, 'ARTIST_DAILY', bt_table_name_artist,  chngDailyDag, df_max_worker_artist)
#15000211


#project  task
t_build_chngset_aggregate_project = createBashOprTask('build-chngset-aggregate-project', 'PROJECT', stage_build, '15000101', '15000211', sst_job_start_date, sst_job_end_date, sst_chngst_start_date, sst_chngst_end_date, 'PROJECT_DAILY', sst_job_load_date, chngDailyDag)

t_load_chngset_aggregate_project = createDataflowOprTask('load-chngset-aggregate-project', 'PROJECT', stage_load, '15000101', '15000211', sst_job_start_date, sst_job_end_date,  'PROJECT_DAILY', bt_table_name_project,  chngDailyDag, df_max_worker_project)
#15000211

t_unload_chngset_aggregate_project = createDataflowOprTask('unload-chngset-aggregate-project', 'PROJECT', stage_unload, '15000101', '15000211', sst_job_start_date, sst_job_end_date,  'PROJECT_DAILY', bt_table_name_project,  chngDailyDag, df_max_worker_project)

#track dag task
t_build_chngset_aggregate_track = createBashOprTask('build-chngset-aggregate-track', 'TRACK', stage_build, '15000101', '15000703', sst_job_start_date, sst_job_end_date, sst_chngst_start_date, sst_chngst_end_date, 'TRACK_DAILY', sst_job_load_date, chngDailyDag)

t_load_chngset_aggregate_track = createDataflowOprTask('load-chngset-aggregate-track', 'TRACK', stage_load, '15000101', '15000703', sst_job_start_date, sst_job_end_date,  'TRACK_DAILY', bt_table_name_track,  chngDailyDag, df_max_worker_track)

t_unload_chngset_aggregate_track = createDataflowOprTask('unload-chngset-aggregate-track', 'TRACK', stage_unload, '15000101', '15000703', sst_job_start_date, sst_job_end_date, 'TRACK_DAILY', bt_table_name_track,  chngDailyDag, df_max_worker_track)



#album  task
'''
t_build_chngset_aggregate_album = createBashOprTask('build-chngset-aggregate-album', 'ALBUM', stage_build, '15000101', '15000103', sst_job_start_date, sst_job_end_date, sst_chngst_start_date, sst_chngst_end_date, 'ALBUM_DAILY', sst_job_load_date, chngDailyDag)

t_load_chngset_aggregate_album = createDataflowOprTask('load-chngset-aggregate-album', 'ALBUM', stage_load, '15000101', '15000103', sst_job_start_date, sst_job_end_date,  'ALBUM_DAILY', bt_table_name_album,  chngDailyDag, df_max_worker_album)
#15000211

t_unload_chngset_aggregate_album = createDataflowOprTask('unload-chngset-aggregate-album', 'ALBUM', stage_unload, '15000101', '15000103', sst_job_start_date, sst_job_end_date,  'ALBUM_DAILY', bt_table_name_album,  chngDailyDag, df_max_worker_album)
'''

#change BT Node size task
t_increase_bt_node_size_load = increaseBTClusterSize(chngDailyDag, 'load')

t_decrease_bt_node_size_load = decreaseBTClusterSize(chngDailyDag, 'load')


#connect tasks pipeline

#t_increase_bt_node_size_unload >> t_unload_chngset_aggregate_artist >> t_unload_chngset_aggregate_project >> t_unload_chngset_aggregate_track >> t_unload_chngset_aggregate_album >> t_decrease_bt_node_size_unload

t_query_artist_chngset >> t_check_artist_chngset >> t_increase_bt_node_size_unload >> t_unload_chngset_aggregate_artist 

t_query_project_chngset >> t_check_project_chngset >> t_increase_bt_node_size_unload >> t_unload_chngset_aggregate_project 

t_query_track_chngset >> t_check_track_chngset >> t_increase_bt_node_size_unload >> t_unload_chngset_aggregate_track 

#t_increase_bt_node_size_unload >> t_unload_chngset_aggregate_album



t_unload_chngset_aggregate_artist >> t_decrease_bt_node_size_unload 

t_unload_chngset_aggregate_project >> t_decrease_bt_node_size_unload

t_unload_chngset_aggregate_track >> t_decrease_bt_node_size_unload

#t_unload_chngset_aggregate_album >> t_decrease_bt_node_size_unload



t_decrease_bt_node_size_unload >> t_build_chngset_aggregate_artist >> t_increase_bt_node_size_load 

t_decrease_bt_node_size_unload >> t_build_chngset_aggregate_project >> t_increase_bt_node_size_load 

t_decrease_bt_node_size_unload >> t_build_chngset_aggregate_track >> t_increase_bt_node_size_load 

#t_decrease_bt_node_size_unload >>  t_build_chngset_aggregate_album >> t_increase_bt_node_size_load


#t_increase_bt_node_size_load >> t_load_chngset_aggregate_artist >> t_load_chngset_aggregate_project >> t_load_chngset_aggregate_track >> t_load_chngset_aggregate_album >> t_decrease_bt_node_size_load


t_increase_bt_node_size_load >> t_load_chngset_aggregate_artist >> t_decrease_bt_node_size_load 

t_increase_bt_node_size_load >> t_load_chngset_aggregate_project >> t_decrease_bt_node_size_load


t_increase_bt_node_size_load >> t_load_chngset_aggregate_track >> t_decrease_bt_node_size_load 

t_decrease_bt_node_size_load  >> trigger_next
#t_increase_bt_node_size_load >> t_load_chngset_aggregate_album >> t_decrease_bt_node_size_load


