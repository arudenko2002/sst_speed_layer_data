package com.umusic.gcp.sst.speedlayer.data.batch;


import com.umusic.gcp.sst.speedlayer.data.entity.ChangeSetConf;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTAggregationLoadQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.queryengine.SSTQueryEngine;
import com.umusic.gcp.sst.speedlayer.data.util.BigtableAdminAPIUtil;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by arumugv on 5/25/17.
 * removes or deletes the changed entities from BT table
 */
public class SSTSpeedLayerAggChangesetEraser extends GenericSSTSpeedLayerGenerator {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerAggChangesetEraser.class);


    @Override
    protected void triggerProcess(SSTSpeedLayerOptions options) throws Exception {

        ChangeSetConf chConf = SSTSpeedLayerUtil.findChangeSetConf(options.getChangeset());

        SSTQueryEngine queryEngine = null;


        long count = SSTSpeedLayerUtil.findChangesetCount(options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate());

        if (count == 0) {
            LOGGER.info("No records found for changeset {}", options.getChangeset());
            // return;
        }


        List<QueryParamEntity> loadQueryList = null;
        Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema = null;
        queryEngine = new SSTAggregationLoadQueryEngine(options.getModule(), options.getLoadDate(), options.getStartDate(),
                options.getEndDate(),
                options.getChangeset(), options.getChangesetStartDate(), options.getChangesetEndDate(), chConf, options.getProductDate());

        //accumulate all delete query list
        if (options.getStartShard() != null && options.getEndShard() != null) {
            loadQueryList = queryEngine.buildDeleteQuery(options.getStartShard(), options.getEndShard());
        } else {
            loadQueryList = queryEngine.buildDeleteQuery(null, null);
        }


        //find all data loading schema/mapping
        dataLoadingSchema = queryEngine.getAllDataLoadingSchema();


        //check if BT table  exists
        BigtableAdminAPIUtil btUtil = new BigtableAdminAPIUtil(options.getBigtableProjectId(), options.getBigtableInstanceId());
        if (!btUtil.checkTableExists(options.getBigtableTableId())) {
            btUtil.closeConnection();
            LOGGER.info("BT table doesn't exist {} ", options.getBigtableTableId());
            return;

        }


        if (loadQueryList != null && loadQueryList.size() > 0) {
            removeAggregationData(options, dataLoadingSchema, loadQueryList);
        }


    }
}
