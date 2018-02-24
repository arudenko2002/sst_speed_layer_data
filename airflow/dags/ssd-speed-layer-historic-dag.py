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

start_date = datetime(2017, 11, 15, 13, 0, 0, tzinfo=pytz.utc)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['Vijay.Arumugaperumal@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)

}

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

def createBashOprTask(task_name, module, stage, startShard, endShard, startDate, endDate, jobLoadDate, dagObj):
    if startShard == "" or endShard == "":
        bashcommand = 'java -jar {{ var.value.sst_jar_base }}{{ var.value.sst_jar_name }} --module=' + module + \
                       ' --stage=' + stage + \
                       ' --startDate=' + startDate + \
                       ' --endDate=' + endDate + \
                       ' --loadDate=' + jobLoadDate + \
                       ' --productDate=' + historic_product_date + \
                       ' --project={{ var.value.gcp_project_id }}'
    else:
        bashcommand = 'java -jar {{ var.value.sst_jar_base }}{{ var.value.sst_jar_name }} --module=' + module + \
                      ' --stage=' + stage + \
                      ' --startShard=' + startShard + \
                      ' --endShard=' + endShard + \
                      ' --startDate=' + startDate + \
                      ' --endDate=' + endDate + \
                      ' --loadDate=' + jobLoadDate + \
                      ' --productDate=' + historic_product_date + \
                      ' --project={{ var.value.gcp_project_id }}'

    return BashOperator(
        task_id=task_name,
        bash_command = bashcommand,
        dag=dagObj

    )

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

# All Dag & Task declartion follows


#artist RTD AGG dag & task
#create new dag
sstRtdDag = createDag('ssd-speed-layer-historic-rtd');

#artist RTD  AGG task
t_build_aggregate_rtd_artist = createBashOprTask('build-aggregate-rtd-artist', 'ARTIST', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#project RTD  AGG task
t_build_aggregate_rtd_project = createBashOprTask('build-aggregate-rtd-project', 'PROJECT', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#track RTD dag AGG task
t_build_aggregate_rtd_track = createBashOprTask('build-aggregate-rtd-track', 'TRACK', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#album RTD dag AGG task
t_build_aggregate_rtd_album = createBashOprTask('build-aggregate-rtd-album', 'ALBUM', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#isrc RTD dag AGG task
t_build_aggregate_rtd_isrc = createBashOprTask('build-aggregate-rtd-isrc', 'ISRC', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#top artist RTD dag AGG task
t_build_aggregate_rtd_top_artist = createBashOprTask('build-aggregate-rtd-top-artist', 'TOPARTIST', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#top project RTD dag AGG task
t_build_aggregate_rtd_top_project = createBashOprTask('build-aggregate-rtd-top-project', 'TOPPROJECT', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

#top track RTD dag AGG task
t_build_aggregate_rtd_top_track = createBashOprTask('build-aggregate-rtd-top-track', 'TOPTRACK', stage_build, '', '', historic_rtd_start_date, historic_rtd_end_date, historic_load_date,  sstRtdDag)

t_build_aggregate_rtd_artist >> t_build_aggregate_rtd_project >> t_build_aggregate_rtd_album >> t_build_aggregate_rtd_track  >> t_build_aggregate_rtd_isrc >> t_build_aggregate_rtd_top_artist >> t_build_aggregate_rtd_top_project >> t_build_aggregate_rtd_top_track





#artist dag & task
#create new dag
artistDag = createDag('ssd-speed-layer-historic-artist');
#change BT Node size isrc task
t_increase_bt_node_size_artist = increaseBTClusterSize(artistDag, 'artist');

t_decrease_bt_node_size_artist = decreaseBTClusterSize(artistDag, 'artist');

#artist dag task
t_build_aggregate_artist = createBashOprTask('build-aggregate-artist', 'ARTIST', stage_build, '', '', historic_start_date, historic_end_date, historic_load_date, artistDag)

t_load_aggregate_artist = createDataflowOprTask('load-aggregate-artist', 'ARTIST', stage_load, '14000101', '15000211', historic_start_date, historic_end_date, bt_table_name_artist, df_startup_worker_node, artistDag)

t_build_aggregate_artist >> t_increase_bt_node_size_artist >> t_load_aggregate_artist >> t_decrease_bt_node_size_artist


#project dag & task
#create new dag
projectDag = createDag('ssd-speed-layer-historic-project');
#change BT Node size isrc task
t_increase_bt_node_size_project = increaseBTClusterSize(projectDag, 'project');

t_decrease_bt_node_size_project = decreaseBTClusterSize(projectDag, 'project');

#project dag task
t_build_aggregate_project = createBashOprTask('build-aggregate-project', 'PROJECT', stage_build, '', '', historic_start_date, historic_end_date, historic_load_date, projectDag)

t_load_aggregate_project = createDataflowOprTask('load-aggregate-project', 'PROJECT', stage_load, '14000101', '15000211', historic_start_date, historic_end_date, bt_table_name_project, df_startup_worker_node, projectDag)

t_build_aggregate_project >> t_increase_bt_node_size_project >> t_load_aggregate_project >> t_decrease_bt_node_size_project



#track dag & task
#create new dag
trackDag = createDag('ssd-speed-layer-historic-track');
#change BT Node size isrc task
t_increase_bt_node_size_track = increaseBTClusterSize(trackDag, 'track');

t_decrease_bt_node_size_track = decreaseBTClusterSize(trackDag, 'track');

#track dag task
t_build_aggregate_track = createBashOprTask('build-aggregate-track', 'TRACK', stage_build, '', '', historic_start_date, historic_end_date, historic_load_date, trackDag)

t_load_aggregate_track = createDataflowOprTask('load-aggregate-track', 'TRACK', stage_load, '14000101', '15000703', historic_start_date, historic_end_date, bt_table_name_track, df_startup_worker_node, trackDag)

t_build_aggregate_track >> t_increase_bt_node_size_track >> t_load_aggregate_track >> t_decrease_bt_node_size_track

#isrc dag & task
#create new dag
isrcDag = createDag('ssd-speed-layer-historic-isrc');
#change BT Node size isrc task
t_increase_bt_node_size_isrc = increaseBTClusterSize(isrcDag, 'isrc');

t_decrease_bt_node_size_isrc = decreaseBTClusterSize(isrcDag, 'isrc');

#isrc dag task
t_build_aggregate_isrc = createBashOprTask('build-aggregate-isrc', 'ISRC', stage_build, '', '', historic_start_date, historic_end_date, historic_load_date, isrcDag)

t_load_aggregate_isrc = createDataflowOprTask('load-aggregate-isrc', 'ISRC', stage_load, '14000101', '15000211', historic_start_date, historic_end_date, bt_table_name_isrc, df_startup_worker_node, isrcDag)

t_build_aggregate_isrc >> t_increase_bt_node_size_isrc >> t_load_aggregate_isrc >> t_decrease_bt_node_size_isrc


# Album dag and task
#create new dag
albumDag = createDag('ssd-speed-layer-historic-album');

#change BT Node size album
t_increase_bt_node_size_album = increaseBTClusterSize(albumDag, 'album');

t_decrease_bt_node_size_album = decreaseBTClusterSize(albumDag, 'album');

#aggreagte and load task
t_build_aggregate_album = createBashOprTask('build-aggregate-album', 'ALBUM', stage_build, '', '', historic_start_date, historic_end_date, historic_load_date, albumDag)

t_load_aggregate_album = createDataflowOprTask('load-aggregate-album', 'ALBUM', stage_load, '14000101', '15000211', historic_start_date, historic_end_date, bt_table_name_album, df_startup_worker_node, albumDag)

#connect tasks pipeline
t_build_aggregate_album >> t_increase_bt_node_size_album >> t_load_aggregate_album >> t_decrease_bt_node_size_album



#top artist dag & task
#create new dag
topArtistDag = createDag('ssd-speed-layer-historic-top-artist');

#change BT Node size top artist
t_increase_bt_node_size_top_artist = increaseBTClusterSize(topArtistDag, 'topartist');

t_decrease_bt_node_size_top_artist = decreaseBTClusterSize(topArtistDag, 'topartist');

#aggreagte and load task
t_build_aggregate_top_artist = createBashOprTask('build-aggregate-top-artist', 'TOPARTIST', stage_build, '15000101', '15000421', historic_start_date, historic_end_date, historic_load_date, topArtistDag);

t_load_aggregate_top_artist = createDataflowOprTask('load-aggregate-top-artist', 'TOPARTIST', stage_load, '15000101', '15000421', historic_start_date, historic_end_date, bt_table_name_top_artist, df_startup_worker_node, topArtistDag);

#connect tasks pipeline
t_build_aggregate_top_artist >> t_increase_bt_node_size_top_artist >> t_load_aggregate_top_artist >> t_decrease_bt_node_size_top_artist



#top project dag & task
#create new dag
topProjectDag = createDag('ssd-speed-layer-historic-top-project');

#change BT Node size top artist
t_increase_bt_node_size_top_project = increaseBTClusterSize(topProjectDag, 'topproject');

t_decrease_bt_node_size_top_project = decreaseBTClusterSize(topProjectDag, 'topproject');

#aggreagte and load task
t_build_aggregate_top_project = createBashOprTask('build-aggregate-top-project', 'TOPPROJECT', stage_build, '15000101', '15001221', historic_start_date, historic_end_date, historic_load_date, topProjectDag)

t_load_aggregate_top_project = createDataflowOprTask('load-aggregate-top-project', 'TOPPROJECT', stage_load, '15000101', '15001221', historic_start_date, historic_end_date, bt_table_name_top_project, df_startup_worker_node, topProjectDag)

#connect tasks pipeline
t_build_aggregate_top_project >> t_increase_bt_node_size_top_project >> t_load_aggregate_top_project >> t_decrease_bt_node_size_top_project


#top track dag & task
#create new dag
topTrackDag = createDag('ssd-speed-layer-historic-top-track')

#change BT Node size top artist
t_increase_bt_node_size_top_track = increaseBTClusterSize(topTrackDag, 'toptrack')

t_decrease_bt_node_size_top_track = decreaseBTClusterSize(topTrackDag, 'toptrack')

#aggreagte and load task
t_build_aggregate_top_track = createBashOprTask('build-aggregate-top-track', 'TOPTRACK', stage_build, '15000101', '15001221', historic_start_date, historic_end_date, historic_load_date, topTrackDag)

t_load_aggregate_top_track = createDataflowOprTask('load-aggregate-top-track', 'TOPTRACK', stage_load, '15000101', '15001221', historic_start_date, historic_end_date, bt_table_name_top_track, df_startup_worker_node, topTrackDag)

#connect tasks pipeline
t_build_aggregate_top_track >> t_increase_bt_node_size_top_track >> t_load_aggregate_top_track >> t_decrease_bt_node_size_top_track







