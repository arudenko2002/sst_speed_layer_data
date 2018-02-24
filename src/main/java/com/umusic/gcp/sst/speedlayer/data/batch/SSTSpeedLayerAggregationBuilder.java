package com.umusic.gcp.sst.speedlayer.data.batch;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTAggregationBuildQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Created by arumugv on 3/9/17.
 * SST speed layer aggregation builder class
 */
public class SSTSpeedLayerAggregationBuilder extends GenericSSTSpeedLayerGenerator {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerAggregationBuilder.class);


    /**
     * buils aggreagtion based on module, start/end date
     *
     * @param options
     * @throws Exception
     */
    @Override
    protected void triggerProcess(SSTSpeedLayerOptions options) throws Exception {

        List<QueryParamEntity> queries = null;


        //create query engine based on module
        SSTQueryEngine queryBuilder = new SSTAggregationBuildQueryEngine(options.getModule(), options.getStartDate(), options.getEndDate(), options.getLoadDate()
                , options.getProductDate(), options.getDynamicDateMode(), options.getDeltaAggMode());

        //build queries for all shards or specific shards
        if (options.getStartShard() != null && options.getEndShard() != null) {
            queries = queryBuilder.buildQuery(options.getStartShard(), options.getEndShard());
        } else {
            queries = queryBuilder.buildQuery(null, null);
        }

        //temp work to write all queries to file
        // SSTSpeedLayerUtil.writeQueriesToFile(options.getModule(), queries);

        //build schema

        Schema schema = queryBuilder.getAggregationTableSchema();

        JobInfo.WriteDisposition writeTrunmode = JobInfo.WriteDisposition.WRITE_TRUNCATE;

        JobInfo.WriteDisposition appendMode = JobInfo.WriteDisposition.WRITE_APPEND;

        if (queries != null && queries.size() > 0) {
            if (SSTSpeedLayerUtil.isTopPerformer(options.getModule())) {
                executeQueriesByBatchForTopPerformers(queries, schema, writeTrunmode);
            } else {
                executeQueriesByBatchForDetail(queries, schema, SSTSpeedLayerUtil.isDeltaAggregationMode(options.getDeltaAggMode()) ? appendMode : writeTrunmode);
            }

        }


    }
}
