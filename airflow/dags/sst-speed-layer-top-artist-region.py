
from __future__ import print_function
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.sensors import HttpSensor


from datetime import datetime, timedelta
import pytz


start_date = datetime(2017, 11, 15, 0, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['dmitri.safin@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)

}


region_shards = [
  '15000105-15000108',
  '15000117-15000120',
  '15000204-15000207',
  '15000216-15000219',
  '15000402-15000405',
  '15000414-15000417']

#define variables
historic_start_date = '{{var.value.sst_historic_start_date}}'
historic_end_date = '{{var.value.sst_historic_end_date}}'

historic_load_date = '{{var.value.sst_historic_load_date}}'

historic_product_date = '{{var.value.sst_historic_product_date}}'

#define variables
historic_rtd_start_date = '{{var.value.sst_historic_rtd_start_date}}'
historic_rtd_end_date = '{{var.value.sst_historic_rtd_end_date}}'
#historic_rtd_load_date = '{{var.value.sst_historic_rtd_load_date}}'

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

#bt_table_name_artist = 'artists_test'
#bt_table_name_track = 'test_track'
#bt_table_name_project = 'test_project'

#BT cluster size
bt_max_cluster_node_size = '{{var.value.sst_gcp_bt_max_cluster_nodes}}'
bt_min_cluster_node_size = '{{var.value.sst_gcp_bt_min_cluster_nodes}}'

#df worker node size
df_max_worker_node = '{{var.value.sst_gcp_df_max_worker_nodes}}'
df_startup_worker_node = '10'
#sst_job_execution_date = '{{ ds }}'

def createDag(dagName):
    return DAG(
        dag_id=dagName,
        schedule_interval=None,
        default_args=default_args
    )

def createBashOprTask(task_name, module, stage, startShard, endShard, startDate, endDate,  dagObj):
    if startShard == "" or endShard == "":
        bashcommand = 'java -jar {{ var.value.sst_jar_base }}{{ var.value.sst_jar_name }} --module=' + module + \
                       ' --stage=' + stage + \
                       ' --startDate=' + startDate + \
                       ' --endDate=' + endDate + \
                       ' --loadDate=' + historic_load_date + \
                       ' --productDate=' + historic_product_date + \
                       ' --project={{ var.value.gcp_project_id }}'
    else:
        bashcommand = 'java -jar {{ var.value.sst_jar_base }}{{ var.value.sst_jar_name }} --module=' + module + \
                      ' --stage=' + stage + \
                      ' --startShard=' + startShard + \
                      ' --endShard=' + endShard + \
                      ' --startDate=' + startDate + \
                      ' --endDate=' + endDate + \
                      ' --loadDate=' + historic_load_date + \
                      ' --productDate=' + historic_product_date + \
                      ' --project={{ var.value.gcp_project_id }}'

    
    return BashOperator(
        task_id=task_name,
        bash_command = bashcommand,
        dag=dagObj

    )
    logging.info('Bash command: %s', bashcommand)
    return bashcommand

def createDataflowOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, bigTableName, numWorkers, dagObj):
    return DataFlowJavaOperator(
        task_id=task_name,
        jar='{{ var.value.sst_jar_base }}{{ var.value.sst_jar_name }}',
        options={
            'project': '{{ var.value.gcp_project_id }}',
            'zone': '{{ var.value.gcp_df_zone }}',
            'stagingLocation': '{{ var.value.staging_location }}',
            'autoscalingAlgorithm': 'THROUGHPUT_BASED',
            'bigtableProjectId': '{{ var.value.sst_gcp_bt_project_id }}',
            'bigtableInstanceId' : '{{ var.value.sst_gcp_bt_instance_id }}',
            'workerMachineType': '{{ var.value.sst_gcp_df_machine_type }}',
            'bigtableTableId' : bigTableName,
            'numWorkers' : numWorkers,
            'defaultWorkerLogLevel' : 'ERROR',
            'workerLogLevelOverrides' : '{"com.umusic.gcp.sst.speedlayer.data":"ERROR"}',
            'module' : module,
            'stage' : stage,
            'startShard' : startShard,
            'endShard' : endShard,
            'startDate': startDate,
            'endDate' : endDate,
            'loadDate' : historic_load_date,
            'productDate' : historic_product_date,
            'maxNumWorkers' : df_max_worker_node
        },
        dag=dagObj

    )

sstRtdDag = createDag('sst-speed-layer-top-artist-region-rtd')
topArtistDag = createDag('sst-speed-layer-top-artist-region')

i = 0
t_build_aggregate_rtd_top_artist = [None] * len(region_shards)
t_build_aggregate_top_artist = [None] * len(region_shards)
t_load_aggregate_top_artist = [None] * len(region_shards)

while i < len(region_shards):
  arr_shard_range = region_shards[i].split('-')
  start_shard = arr_shard_range[0]
  end_shard = arr_shard_range[1]

  #top artist RTD dag AGG task
  t_build_aggregate_rtd_top_artist[i] = createBashOprTask(
    'build-aggregate-rtd-top-artist-{}'.format(
      region_shards[i]
    ), 'TOPARTIST', stage_build, start_shard, end_shard, historic_rtd_start_date, historic_rtd_end_date,  sstRtdDag)
  
  #top artist historic AGG task
  t_build_aggregate_top_artist[i] = createBashOprTask('build-aggregate-top-artist-{}'.format(
      region_shards[i]
    ), 'TOPARTIST', stage_build, start_shard, end_shard, historic_start_date, historic_end_date, topArtistDag)

  #top artist historic LOAD task
  t_load_aggregate_top_artist[i] = createDataflowOprTask('load-aggregate-top-artist-{}'.format(
      region_shards[i]
    ), 'TOPARTIST', stage_load, start_shard, end_shard, historic_start_date, historic_end_date, bt_table_name_top_artist, df_startup_worker_node, topArtistDag)

  """
  if i > 0:
    t_build_aggregate_rtd_top_artist[i-1] >> t_build_aggregate_rtd_top_artist[i]
    t_build_aggregate_top_artist[i-1] >> t_build_aggregate_top_artist[i]
    t_load_aggregate_top_artist[i-1] >> t_load_aggregate_top_artist[i]
  """
  
  t_build_aggregate_top_artist[i] >> t_load_aggregate_top_artist[i]

  i += 1  # This is the same as count = count + 1



  









