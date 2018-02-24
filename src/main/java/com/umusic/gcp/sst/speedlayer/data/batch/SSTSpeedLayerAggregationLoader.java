package com.umusic.gcp.sst.speedlayer.data.batch;

import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;

import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTAggregationLoadQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.util.BigtableAdminAPIUtil;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * Created by arumugv on 2/21/17.
 * SST Speed layer Aggregation loader
 */

public class SSTSpeedLayerAggregationLoader extends GenericSSTSpeedLayerGenerator {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerAggregationLoader.class);


    /**
     * Triggers loading process of aggregation to BT
     *
     * @param options
     * @throws Exception
     */
    @Override
    protected void triggerProcess(SSTSpeedLayerOptions options) throws Exception {


        List<QueryParamEntity> queries = null;

        //build query engine based on module
        SSTQueryEngine queryBuilder = new SSTAggregationLoadQueryEngine(options.getModule(), options.getStartDate(),
                options.getEndDate(),
                options.getLoadDate(), options.getProductDate(), options.getDynamicDateMode(), options.getDeltaAggMode());

        //build queries for all shards or specific shards
        if (options.getStartShard() != null && options.getEndShard() != null) {
            queries = queryBuilder.buildQuery(options.getStartShard(), options.getEndShard());
        } else {
            queries = queryBuilder.buildQuery(null, null);
        }

        //find all data loading schema/mapping
        Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema = queryBuilder.getAllDataLoadingSchema();


        //create BT table if doesn't exists
        BigtableAdminAPIUtil btUtil = new BigtableAdminAPIUtil(options.getBigtableProjectId(), options.getBigtableInstanceId());
        if (!btUtil.checkTableExists(options.getBigtableTableId())) {
            LOGGER.info("BT table doesn't exist {} ", options.getBigtableTableId());
            LOGGER.info("creating BT table in {} , {}, ", options.getBigtableProjectId(), options.getBigtableInstanceId());
            btUtil.createTable(options.getBigtableTableId(), queryBuilder.getBTSchema());
        }
        btUtil.closeConnection();


        //build lookup by querying BQ
        Map<String, Map<String, List<String>>> lookUp = SSTSpeedLayerUtil.buildLookup();


        if (queries != null && queries.size() > 0) {
            loadAggregationbyCombinedPcoll(options, dataLoadingSchema, lookUp, queries);
        }


    }
}
