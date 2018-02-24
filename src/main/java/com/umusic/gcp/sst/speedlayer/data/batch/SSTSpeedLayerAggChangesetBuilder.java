package com.umusic.gcp.sst.speedlayer.data.batch;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTAggregationBuildQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTQueryEngine;

import com.umusic.gcp.sst.speedlayer.data.entity.ChangeSetConf;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the changeset in BQ table
 *  Created by arumugv on 5/25/17.
 */
public class SSTSpeedLayerAggChangesetBuilder extends GenericSSTSpeedLayerGenerator {


    @Override
    protected void triggerProcess(SSTSpeedLayerOptions options) throws Exception {

        ChangeSetConf chConf = SSTSpeedLayerUtil.findChangeSetConf(options.getChangeset());


        SSTQueryEngine queryEngine = null;


        long count = SSTSpeedLayerUtil.findChangesetCount(options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate());

        if (count == 0) {
            LOGGER.info("No records found for changeset {}", options.getChangeset());
            return;
        }


        List<List<QueryParamEntity>> alternateDeleteQuryList = new ArrayList<List<QueryParamEntity>>();

        List<List<QueryParamEntity>> buildQryDblList = new ArrayList<List<QueryParamEntity>>();


        queryEngine = new SSTAggregationBuildQueryEngine(options.getModule(), options.getStartDate(), options.getEndDate(), options.getLoadDate(),
                options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate(), chConf, options.getProductDate());


        //queries to delete units applying negation and appending the records
        alternateDeleteQuryList.add(queryEngine.buildDeleteQuery(options.getStartShard(), options.getEndShard()));

        //queries to accumulate all build query list
        buildQryDblList.add(queryEngine.buildQuery(options.getStartShard(), options.getEndShard()));


        //flatten all to changeset querylist for all impacted pages
        List<QueryParamEntity> chngSetTmpQryFlatLst = alternateDeleteQuryList.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        //flatten all build querylist for all impacted pages
        List<QueryParamEntity> buildQryFlatLst = buildQryDblList.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());


        JobInfo.WriteDisposition appendMode = JobInfo.WriteDisposition.WRITE_APPEND;


        //build schema
        Schema schema = queryEngine.getAggregationTableSchema();

        //execute  queries to delete impacted records in agg table by applying negation and appending the record.
        if (chngSetTmpQryFlatLst != null && chngSetTmpQryFlatLst.size() > 0) {
            LOGGER.info("Appending records with negative units to delete");
            executeALLQueriesPeriodTypes(chngSetTmpQryFlatLst, schema, appendMode);
        }


        //execute build queries to add changeset delta in batches based on period types
        if (buildQryFlatLst != null && buildQryFlatLst.size() > 0) {
            LOGGER.info("initiaing all shard queries to recalculate the entities impacted and append to changeset table");
            executeQueriesByBatchForDetail(buildQryFlatLst, null, appendMode);
        }


    }
}
