#!/usr/bin/env bash

#artist_daily_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_daily_by_territory.avsc --bigTableMappingFile=bigtable-mapping/artist_daily_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_daily_by_territory/*"

#artist_weekly_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_weekly_by_territory.avsc --bigTableMappingFile=bigtable-mapping/artist_weekly_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_weekly_by_territory/*"


#artist_rtd_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_rtd_by_territory.avsc --bigTableMappingFile=bigtable-mapping/artist_rtd_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_rtd_by_territory/*"


#artist_daily
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_daily.avsc --bigTableMappingFile=bigtable-mapping/artist_daily_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_daily/*"



#artist_weekly
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_weekly.avsc --bigTableMappingFile=bigtable-mapping/artist_weekly_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_weekly/*"


#artist_rtd
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_rtd.avsc --bigTableMappingFile=bigtable-mapping/artist_rtd_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_rtd/*"



#artist_weekly_by_territory(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_weekly_by_territory_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_weekly_by_territory_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_weekly_by_territory_nopartner/*"

#artist_daily_by_territory(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_by_territory_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_by_territory_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_daily_by_territory_nopartner/*"

#artist_rtd_by_territory(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_by_territory_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_by_territory_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_rtd_by_territory_nopartner/*"

#artist_daily(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_daily_nopartner/*"

#artist_weekly(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_weekly_nopartner/*"

#artist_rtd(no partner)
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist --inputAvroSchemaFileName=avro-schema/artist_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_rtd_nopartner/*"



#project_daily_by_territory_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_daily_by_territory_partner/*"

#project_weekly_by_territory & partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_weekly_by_territory_partner/*"


#project_rtd_by_territory & partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_rtd_by_territory_partner/*"

#project_daily_by_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_daily_by_partner/*"

#project_weekly by partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_weekly_by_partner/*"

#project_rtd_by_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_rtd_by_partner/*"

#project_daily_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_daily_by_territory/*"

#project_weekly by territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_weekly_by_territory/*"

#project_rtd_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_rtd_by_territory/*"


#project_daily
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_daily/*"

#project_weekly
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_weekly/*"


#project_rtd
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=project --inputAvroSchemaFileName=avro-schema/project_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/project_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/project_rtd/*"



##tracks

#tracks_daily_by_territory_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_daily_by_territory_partner/*"

#tracks_weekly_by_territory_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_weekly_by_territory_partner/*"

#tracks_rtd_by_territory_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_rtd_by_territory_partner/*"

#tracks_daily_by_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_daily_by_partner/*"

#tracks_weekly_by_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_weekly_by_partner/*"

#tracks_rtd_by_partner
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_partner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_rtd_by_partner/*"


#tracks_daily_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_daily_by_territory/*"

#tracks_weekly_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_weekly_by_territory/*"

#tracks_rtd_by_territory
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_rtd_by_territory/*"


#tracks_daily
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_daily/*"

#tracks_weekly
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_weekly/*"

#tracks_rtd
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=track --inputAvroSchemaFileName=avro-schema/track/track_by_territory_partner.avsc --bigTableMappingFile=bigtable-mapping/track/track_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/track_rtd/*"


#top_track

by territory/partner/genre/label
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_partner_genre_label/*"


gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_partner/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_genre/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_partner/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_genre/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_partner_genre/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_partner_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_genre_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_partner_genre/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_partner_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_territory_genre_label/*"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=top_tracks --inputAvroSchemaFileName=avro-schema/top_track1.avsc --bigTableMappingFile=bigtable-mapping/top_track1.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/vijay_top_track/top_track_weekly_by_partner_genre_label/*"


#test_artists_daily_by_territory

#gs://umg-dev/data/consumption/artist_daily_by_territory/*


#sample

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist_test --inputAvroSchemaFileName=avro-schema/artist_by_territory_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_by_territory_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/samples/artist_daily_By_territory_nopartner_sample.avro"


gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist_rtd_test --inputAvroSchemaFileName=avro-schema/artist_rtd_by_territory.avsc --bigTableMappingFile=bigtable-mapping/artist_rtd_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/samples/artist_rtd_By_territory_sample.avro"


gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist_rtd_test --inputAvroSchemaFileName=avro-schema/artist_by_territory_nopartner.avsc --bigTableMappingFile=bigtable-mapping/artist_by_territory_nopartner_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/samples/artist_rtd_by_territory_nopartner_sample.avro"

--workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"INFO\"}

//first stage agg
gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR  --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --module=TOPARTIST --startShard=15000117 --endShard=15000124 --autoscalingAlgorithm=NONE --numWorkers=20"

gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR  --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --module=ARTIST --startShard=15000102 --endShard=15000103 --autoscalingAlgorithm=NONE --numWorkers=20"

--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR  --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --page=ARTIST --stage=FIRST --startShard=15000101 --endShard=15000101 --autoscalingAlgorithm=NONE --numWorkers=20

//second stage agg big table
-PmainClassName="com.umusic.gcp.sst.speedlayer.data.batch.BigtableAggregationDataGenrator"
gradle run  -Pargs="--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --bigtableProjectId=umg-staging --bigtableInstanceId=consumption-swift  --bigtableTableId=top_artist --module=TOPARTIST --startShard=15000101 --endShard=15000104 --autoscalingAlgorithm=NONE --numWorkers=30"

--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --bigtableProjectId=umg-staging --bigtableInstanceId=consumption-swift  --bigtableTableId=top_artist --page=TOPARTIST --stage=SECOND --startShard=15000117 --endShard=15000120 --autoscalingAlgorithm=NONE --numWorkers=30

//bigtable
--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"DEBUG\"} --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift  --bigtableTableId=artist --page=ARTIST --stage=SECOND --startShard=15000106 --endShard=15000101 --autoscalingAlgorithm=NONE --numWorkers=30 --zone=us-central1-c

--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR --workerLogLevelOverrides={\"com.umusic.gcp.sst.speedlayer.data\":\"DEBUG\"} --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift  --bigtableTableId=track --page=TRACK --stage=SECOND --startShard=15000106 --endShard=15000110 --autoscalingAlgorithm=NONE --numWorkers=30 --zone=us-central1-c


//big query
--runner=BlockingDataflowPipelineRunner --defaultWorkerLogLevel=ERROR  --project=umg-staging --stagingLocation=gs://umg-staging/temp/dataflow --page=TOPARTIST --stage=FIRST --startShard=15000125 --endShard=15000125 --autoscalingAlgorithm=NONE --numWorkers=20



--project=umg-dev --stagingLocation=gs://umg-staging/temp/dataflow --page=ARTIST --stage=FIRST --startShard=15000101 --endShard=15000101 --startDate=20160701 --endDate=20160705