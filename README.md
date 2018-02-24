# sst-speed-layer-data
This project for finding top performers and their detail consumption units by period/territory/partner/label for umg swift consumption.

#### Synopsis 
+ Builds aggregation of consumption by period/territory/partner/label/genre/catalogue and rank them. 
+ Load consumption units to Big table.


#### Technology Stack/libraries
+ java 8
+ Bigquery Java API(google-cloud-bigquery:0.9.4-beta)
+ google-cloud-dataflow-java-sdk-all:1.9.1
+ bigtable-hbase-dataflow:0.9.4
+ jackson-dataformat-yaml
+ bigtable-hbase-1.x
+ jackson-databind
+ gradle 
+ slf4j & logback

#### Modules
+ ARTIST 
+ PROJECT
+ TRACK
+ ALBUM
+ ISRC
+ TOPARTIST
+ TOPPROJECT
+ TOPTRACK

#### Stages
+ BUILD
+ LOAD
+ CHANGESETBUILD
+ CHANGESETLOAD
+ CHANGESETUNLOAD

#### Building jar
 gradle clean build fatjar
 


#### Job Execution params

###### Historical Building Aggregation

    java -jar sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --module={MODULE_NAME} --stage=BUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --productDate={PRODUCT_DATE} --loadDate={LOAD_DATE}

NOTE: startShard AND ENDSHARD are non mandatory

##### Daily Incremental job

Daily incremental mode is also known as dynamic date mode. it can be Executed in four  different ways . first two based on decision engine off mode, identfying the report date impacted from transaction table.
Decision engine is turned on/off using the param in decisionEngineEnabled = true/false in sst-configuration.yaml file.

**Decision Engine - Off** (not currently used) 

a. Dynamic/Incremental date mode(decision engine off)  - finds the report days impacted from transactions table based on {START_DATE} <= transactions_load_date <= {END_DATE}   


    java -jar sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --module={MODULE_NAME} --stage=BUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --productDate={PRODUCT_DATE} --loadDate={LOAD_DATE} --dynamicDateMode=Y

b. Dynamic/Incremental date mode with delta Aggregation (decision engine off)  -   finds the report days impacted from transactions table based on {START_DATE} <= transactions_load_date <= {END_DATE}, Aggregates only the delta record loaded that day(not applicable for top performers)


    java -jar sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --module={MODULE_NAME} --stage=BUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --productDate={PRODUCT_DATE} --loadDate={LOAD_DATE} --dynamicDateMode=Y --deltaAggMode=Y

**Decision Engine - On** (currently in  use) 


c. Dynamic/Incremental date mode(with decision engine on)  - finds the report days impacted from decision engine table.  


    java -jar sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --module={MODULE_NAME} --stage=BUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --productDate={PRODUCT_DATE} --loadDate={LOAD_DATE} --dynamicDateMode=Y


d. Dynamic/Incremental date mode with delta Aggregation(decision engine on) - finds the report days impacted from decision engine table.  Aggregates only the delta record loaded that day(not applicable for top performers)


    java -jar sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --module={MODULE_NAME} --stage=BUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --productDate={PRODUCT_DATE} --loadDate={LOAD_DATE} --dynamicDateMode=Y --deltaAggMode=Y
    
 **Note:** Decision engine is turned on/off using the param in decisionEngineEnabled = true/false in sst-configuration.yaml file.
    


###### Historical  Loading Aggregation

    java -jar build/libs/sst-speed-layer-1.0-SNAPSHOT.jar --runner=DataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"ERROR\"} --project={PROJECT_NAME} --stagingLocation={STAGING_LOCATION} --bigtableProjectId={BT_PROJ_ID} --bigtableInstanceId={BT_INSTANCE_ID}  --bigtableTableId={BT_TABLE_ID} --module={MODULE_NAME} --stage=LOAD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --loadDate={LOAD_DATE} --productDate={PRODUCT_DATE} --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers={INITIAL_DF_WORKER}  --workerMachineType={MACHINE_TYPE}--maxNumWorkers={MAX_DF_WORKER}

###### Daily Incremental  Loading Aggregation

    java -jar build/libs/sst-speed-layer-1.0-SNAPSHOT.jar --runner=DataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"ERROR\"} --project={PROJECT_NAME} --stagingLocation={STAGING_LOCATION} --bigtableProjectId={BT_PROJ_ID} --bigtableInstanceId={BT_INSTANCE_ID}  --bigtableTableId={BT_TABLE_ID} --module={MODULE_NAME} --stage=LOAD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --loadDate={LOAD_DATE} --productDate={PRODUCT_DATE} --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers={INITIAL_DF_WORKER}  --workerMachineType={MACHINE_TYPE} --maxNumWorkers={MAX_DF_WORKER} --dynamicDateMode=Y

###### CHANGESET Aggregation(BUILD)

    java -jar build/libs/sst-speed-layer-1.0-SNAPSHOT.jar --project={PROJECT_NAME} --stagingLocation={STAGING_LOCATION} --module={MODULE_NAME} --stage=CHANGESETBUILD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --changesetStartDate={CHNGSET_START_DATE} --changesetEndDate={CHNGSET_END_DATE} --loadDate={LOAD_DATE} --changeset={CHNGSET_TYPE} --productDate={PRODUCT_DATE}

###### CHANGESET Aggregation(UNLOAD) 

    java -jar build/libs/sst-speed-layer-1.0-SNAPSHOT.jar --runner=DataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"ERROR\"} --project={PROJECT_NAME} --stagingLocation={STAGING_LOCATION} --bigtableProjectId={BT_PROJ_ID} --bigtableInstanceId={BT_INSTANCE_ID}  --bigtableTableId={BT_TABLE_ID} --module={MODULE_NAME} --stage=CHANGESETUNLOAD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --loadDate={LOAD_DATE} --productDate={PRODUCT_DATE} --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers={INITIAL_DF_WORKER}  --workerMachineType={MACHINE_TYPE}--maxNumWorkers={MAX_DF_WORKER} --changesetStartDate={CHNGSET_START_DATE} --changesetEndDate={CHNGSET_END_DATE} --changeset={CHNGSET_TYPE}

###### CHANGESET Aggregation(LOAD) 

    java -jar build/libs/sst-speed-layer-1.0-SNAPSHOT.jar --runner=DataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"ERROR\"} --project={PROJECT_NAME} --stagingLocation={STAGING_LOCATION} --bigtableProjectId={BT_PROJ_ID} --bigtableInstanceId={BT_INSTANCE_ID}  --bigtableTableId={BT_TABLE_ID} --module={MODULE_NAME} --stage=CHANGESETLOAD --startShard={SHARD_NUMBER} --endShard={SHARD_NUMBER} --startDate={START_DATE} --endDate={END_DATE} --loadDate={LOAD_DATE} --productDate={PRODUCT_DATE} --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers={INITIAL_DF_WORKER}  --workerMachineType={MACHINE_TYPE}--maxNumWorkers={MAX_DF_WORKER} --changesetStartDate={CHNGSET_START_DATE} --changesetEndDate={CHNGSET_END_DATE} --changeset={CHNGSET_TYPE}

#### Periodic Table for module
| Modules    | Daily | Weekly | Monthly | Quarterly | Yearly | RTD | Note                                                   |
|------------|-------|--------|---------|-----------|--------|-----|--------------------------------------------------------|
| ARTIST     | Yes   | Yes    | Yes     |           |        | Yes | Daily agg from 2017-03-03,  Weekly Agg till 2017-03-02 |
| PROJECT    | Yes   | Yes    | Yes     |           |        | Yes | Daily agg from 2017-03-03 Weekly Agg till 2017-03-02   |
| TRACK      | Y     | Yes    | Yes     |           |        | Yes | Daily agg from 2017-03-03 Weekly Agg till 2017-03-02   |
| ALBUM      | Yes   | Yes    | Yes     |           |        | Yes | Daily agg from 2017-03-03Weekly Agg till 2017-03-02    |
| ISRC       | Yes   | Yes    | Yes     |           |        | Yes | Daily agg from 2017-03-03Weekly Agg till 2017-03-0     |
| TOPARTIST  |       | YES    | YES     | YES       | YES    |     |                                                        |
| TOPPROJECT |       | YES    | YES     | YES       | YES    |     |                                                        |
| TOPTRACK   |       | YES    | YES     | YES       | YES    |     |                                                        |  


#### Shard
   + Every unique combination of aggregation is flagged/stored as separate shard.  
   + shard configuration files are of json format.
   + shard files are located under **resources/shard** directory. 
   + shard numbers assigned are some old dates in 1500's and 1400's. reason is bigquery provides a option of sharding table based on date.   
   + Aggregation dataset,table name , project(not used) and BT table name variables are set in shard file.  
   + BT Data loading type is set in the shard file.
   + parentshard and crossmoduleparentshard configuration is set in shard file  
   
   | Module      | Shard File Name        |
   |-------------|------------------------|
   | Artist      | artist-shard.json      |
   | project     | project-shard.json     |
   | track       | track-shard.json       |
   | album       | album-shard.json       |
   | isrc        | isrc-shard.json        |
   | Top Track   | top-track-shard.json   |
   | Top Project | top-project-shard.json |
   | Top Artist  | top-artist-shard.json  |

  Note: there are some old shard file like split by country sub division and isrc splits. these are not in use
  
#### Application Config
   + yaml files is used as configuration. file name is sst-configuration.yaml.
   + located under **resources** directory. 
   + jackson-dataformat-yaml library converts this yml to java entity.
   + look up queries, changeset related queries, dataset, table names and table alias name are stored in this config file.  
   + flag to enable decision engine. 
   + flag to enable weekly for periods before march 2017 in case of detail module. 
   + flag to enable cross module build.  
   
#### Design Document Links
   * [Consumption SST speed layer Aggregations](https://docs.google.com/spreadsheets/d/15e5Xc6SAMCuGE8-LJnk1sApLtzK5iKYos9djVhsRl_8/edit?usp=drive_web)  
   * [Consumption SST speed layer Big table](https://docs.google.com/spreadsheets/d/1bvKekw84YIejLD6QB5Jmc0J_DQb0QQ5EmytVxxc1l4w/edit?usp=drive_web)





#### Historical/incremental Process
  Involves two step process.  
    1. Build-Aggregation(BUILD)  
    2. Load Aggregation

##### Build Aggregation(BUILD): 
  + Aggregations are build in Bigquery  and stored in Bigquery sharded table.
  + Periodic Aggregations parent child relationships    
    
    | Child Aggreagtion Type | Parent Aggregation Type |
    |------------------------|-------------------------|
    | Monthly                | Daily                   |
    | Quarterly              | Monthly                 |
    | Yearly                 | Quarterly               |
    | RTD                    | Monthly                 |
    
    Note: Daily and weekly are build from transactions and other metadata table.
  
  + All Periodic aggreagtion except RTD is stored in partition table in respective partition, here is table partition details  
    
    | Aggregation Type | Is Partitioned ?  | Partition Info                                        |
    |------------------|-------------------|-------------------------------------------------------|
    | Daily            | Yes               | Aggregation stored in daily partition                 |
    | Weekly           | Yes               | Aggregation stored in start date of every global week |
    | Monthly          | Yes               | Aggregation stored in start date of every month       |
    | Quarterly        | Yes               | Aggregation stored in start date of every quarter     |
    | Yearly           | Yes               | Aggregation stored in start date of every year        |
    | RTD              | NO                |                                                       |
    
  + Periodic details for day, month, global week , yearly like start date ,end date are obtained from day table.  
  + Bigquery doesn't support dynamic partition alternate option is genrate and execute Queries for every single periodic partition.  
  + Historical Job  
    * Input is start date and end date.  
    * Generate and execute queries for all periodic partition in range of dates between start and end date.  
    * Aggregation table write disposition mode is WriteTruncate.
    
  + Daily Incremental Job
    
    | Options                             | dynamicDateMode | deltaAggMode    | Decision Engine | PROCESS                                                                                                                                                                                                                                                                                                                                                         |
    |-------------------------------------|-----------------|-----------------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Dynamic date mode                   | Y               | N               | Off             | finds the report days impacted from transactions table based on {START_DATE} <= transactions_load_date <= {END_DATE}<br/> Generate and execute queries for all periodic partition in distinct set of impacted dates. <br/>Aggregation table write disposition mode is WriteTruncate.                                                                            |
    | Dynamic date mode With deltaAGG     | Y               | Y               | Off             | finds the report days impacted from transactions table based on {START_DATE}<= transactions_load_date <= {END_DATE} <br/>Generate and execute queries for all periodic partition in distinct set of impacted dates <br/> AGGregate only delta records {START_DATE} <= transactions_date <= {END_DATE} <br/> Aggregation table write disposition mode is append. |
    | Dynamic date mode                   | Y               | N               | ON              | finds the report days impacted from decision engine table. <br/> Generate and execute queries for all periodic partition in distinct set of impacted dates <br/> Aggregation table write disposition mode is WriteTruncate.                                                                                                                                     |
    | Dynamic date mode With deltaAGG     | Y               | Y               | ON              | finds the report days impacted from decision table <br/>,Generate and execute queries for all periodic partition in distinct set of impacted dates<br/>AGGregate only delta records {START_DATE} <= transactions_date <= {END_DATE} <br/>Aggregation table write disposition mode is append.                                                                    |
  
  
  + configs for building aggregation like sqls, table schema are located in **resources/aggregation-build** directory. 
  + Cross Module aggregation parent child relationship. 
    * TOPARTIST build from ARTIST
    * TOPTRACK build from TRACK
    * TOPPROJECT Build from PROJECT
    * Note: detail module and top performer are not exactly same, very few shards are build on cross module basis.
    
  + start shardnumber and endShardNumber are optional arguments, if not provided, all shards are aggregated.
    
##### Load Aggregation(LOAD): 
  + Aggregations are loaded to google Bigtable using Dataflow. 
  + Loading related configs like sql, mapping file and BT table schema are loacted in **resources/aggregation-load** directory. 
  + BT keys and column family mappping configs are located in **mapping** directory. 
  + Some modules have multiple mapping file bcoz of breakdown by country, subdivision, parters etc.  
  + In historic job -> generate queries for every single shard and filter by START_DATE <= partition <=END_DATE and LOAD_DATETIME = {CUR_LOAD_TIME} 
  + In Incremantal/Dynamic date job -> generate queries for every single shard and respective impacted partition and filter by LOAD_DATETIME = {CUR_LOAD_TIME}. 
  
  + In case of top performer top 500 ranks are loaded to BT  
  + In detail module, if all consumption units are zero, then entire record is not loaded to BT. 
  + start shardnumber and endShardNumber are optional arguments, if not provided, all shards are loaded.  
  
  
#### Chnageset Process  
   * This ELT job captures any chnages in products and updates the corresponding aggreagtion entities. 
   * Executes Daily and weekly to update the consumption units. 
   * Daily changeset process is plan to update 28 days and corrsponding month and RTD aggreagtion. 
   * weekly changest process is plan to update entire dataset from time begin(pending on decision)

   ##### Changeset Modules
   * ARTIST
   * PROJECT 
   * TRACK 
   
   ##### Changeset Types 
   * ARTIST_WEEKLY 
   * ARTIST_DAILY
   * PROJECT_DAILY 
   * PROJECT_WEEKLY
   * TRACK_DAILY 
   * TRACK_WEEKLY 
   * REGION 
   
   ##### Changeset Stages
   * Step 1 : CHANGESETUNLOAD - Unload affected entities from BT. 
   * Steo 2 : CHANGESETBUILD - Remove old aggregation and build new aggreation of affected entities based on latest products. 
   * Step 3 : CHANGESETUNLOAD - Load affected entties to BT. 
   
   ##### Configs 
   * changset related configs are located under **resources/aggregation-changeset**. 
   * **resources/aggregation-changeset/sql** directory - sql to unload entities from BT and sql to negate the consumption units in aggregation table. 
   * changeset.json file is main configuration file related to changeset
        * Contains SQl expression field (field name along with sql expressions) related to entities.
                
                Source-Staging -  field name in the aggregation table 
                Source-Metadata - field name in the transaction and metadata table
                
        * In case of region changeset, only certain shards related to region needs to upddated. This config provides  filter option based on action
           unload, build or load. 
        * In each action there is array of combination for which chngeset needs to be applied and their respective sqlExpField, This sqlExpField will override
          sqlExpField declared at the parent level. it also has it respective dataloading types.  
        * sst-configuration file contains genric sql related to changest like count the number of changeset record in the respective partition, extract list of id's modified in each changeset. 
        
        
                
                
                
        # sst_speed_layer_data
