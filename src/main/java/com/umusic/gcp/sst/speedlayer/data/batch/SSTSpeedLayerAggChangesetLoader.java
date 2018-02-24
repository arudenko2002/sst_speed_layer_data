package com.umusic.gcp.sst.speedlayer.data.batch;


import com.umusic.gcp.sst.speedlayer.data.entity.ChangeSetConf;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTAggregationLoadQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.util.BigtableAdminAPIUtil;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;

import java.util.List;
import java.util.Map;

/**
 * Created by arumugv on 5/25/17.
 * Loads the aggreagted changeset to BT
 */
public class SSTSpeedLayerAggChangesetLoader extends GenericSSTSpeedLayerGenerator {


    @Override
    protected void triggerProcess(SSTSpeedLayerOptions options) throws Exception {

        ChangeSetConf chConf = SSTSpeedLayerUtil.findChangeSetConf(options.getChangeset());

        //String chngSql = loadChangeSetSql(options.getModule());
        SSTQueryEngine queryEngine = null;

        long count = SSTSpeedLayerUtil.findChangesetCount(options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate());

        if (count == 0) {
            LOGGER.info("No records found for changeset {}", options.getChangeset());
            //return;
        }

        //build lookup by querying BQ
        Map<String, Map<String, List<String>>> lookUp = SSTSpeedLayerUtil.buildLookup();


        List<QueryParamEntity> loadQueryList = null;
        Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema = null;
        queryEngine = new SSTAggregationLoadQueryEngine(options.getModule(), options.getLoadDate(), options.getStartDate(),
                options.getEndDate(),
                options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate(), chConf, options.getProductDate());

        //accumulate all build query list
        if (options.getStartShard() != null && options.getEndShard() != null) {
            loadQueryList = queryEngine.buildQuery(options.getStartShard(), options.getEndShard());
        } else {
            loadQueryList = queryEngine.buildQuery(null, null);
        }

        //find all data loading schema/mapping
        dataLoadingSchema = queryEngine.getAllDataLoadingSchema();

        //create BT table if doesn't exists
        BigtableAdminAPIUtil btUtil = new BigtableAdminAPIUtil(options.getBigtableProjectId(), options.getBigtableInstanceId());
        if (!btUtil.checkTableExists(options.getBigtableTableId())) {
            LOGGER.info("BT table doesn't exist {} ", options.getBigtableTableId());
            LOGGER.info("creating BT table in {} , {}, ", options.getBigtableProjectId(), options.getBigtableInstanceId());
            btUtil.createTable(options.getBigtableTableId(), queryEngine.getBTSchema());
        }
        btUtil.closeConnection();

        if (loadQueryList != null && loadQueryList.size() > 0) {
            //load all impacted shards in single DF job
            loadAggregationbyCombinedPcoll(options, dataLoadingSchema, lookUp, loadQueryList);
        }


    }
}
