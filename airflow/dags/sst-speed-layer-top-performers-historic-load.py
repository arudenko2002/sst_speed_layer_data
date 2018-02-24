
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
    'retries': 5,
    'retry_delay': timedelta(minutes=2)

}


modules = ['TOPARTIST', 'TOPPROJECT', "TOPTRACK"]


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

#BT cluster size
bt_max_cluster_node_size = '{{var.value.sst_gcp_bt_max_cluster_nodes}}'
bt_min_cluster_node_size = '{{var.value.sst_gcp_bt_min_cluster_nodes}}'

#df worker node size
df_max_worker_node = '{{var.value.sst_gcp_df_max_worker_nodes}}'
df_startup_worker_node = '10'
#sst_job_execution_date = '{{ ds }}'

bt_names = [bt_table_name_top_artist, bt_table_name_top_project, bt_table_name_top_track]

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


topPerformerDag = createDag('sst-speed-layer-top-performers-historic-load')

t_load_aggregate_top_artist = createDataflowOprTask('load-aggregate-top-artist', 'TOPARTIST', stage_load, '15000101', '15000421', historic_start_date, historic_end_date, bt_table_name_top_artist, df_startup_worker_node, topPerformerDag)
t_load_aggregate_top_project = createDataflowOprTask('load-aggregate-top-project', 'TOPPROJECT', stage_load, '15000101', '15001221', historic_start_date, historic_end_date, bt_table_name_top_project, df_startup_worker_node, topPerformerDag)
t_load_aggregate_top_track = createDataflowOprTask('load-aggregate-top-track', 'TOPTRACK', stage_load, '15000101', '15001221', historic_start_date, historic_end_date, bt_table_name_top_track, df_startup_worker_node, topPerformerDag)


t_increase_bt_node_size = increaseBTClusterSize(topPerformerDag)

t_decrease_bt_node_size = decreaseBTClusterSize(topPerformerDag)

t_increase_bt_node_size >> t_load_aggregate_top_artist >> t_load_aggregate_top_project >> t_load_aggregate_top_track >> t_decrease_bt_node_size

