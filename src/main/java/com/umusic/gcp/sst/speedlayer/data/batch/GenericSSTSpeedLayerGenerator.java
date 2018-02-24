package com.umusic.gcp.sst.speedlayer.data.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.parser.TableRowToDeleteMutationParser;
import com.umusic.gcp.sst.speedlayer.data.parser.TableRowToPutMutationParser;
import com.umusic.gcp.sst.speedlayer.data.util.BigQueryLoadAPIUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



/**
 * Created by arumugv on 3/27/17.
 * Generic Abstract class for SST Speed layer
 */
public abstract class GenericSSTSpeedLayerGenerator {

    public static final Logger LOGGER = LoggerFactory.getLogger(GenericSSTSpeedLayerGenerator.class);
    protected static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Abstract method, child class should implement the process.
     *
     * @param options
     * @throws Exception
     */
    protected abstract void triggerProcess(SSTSpeedLayerOptions options) throws Exception;

    /**
     * Kicks off the trigger process
     *
     * @param options
     * @throws Exception
     */
    public final void initiateProcess(SSTSpeedLayerOptions options) throws Exception {
        triggerProcess(options);
    }


    /**
     * Execute Queries by batches for detail module, splitting them based on interavals, starting from day, week,month, quarter, year, RTD
     *
     * @param queries
     * @param schema
     * @param mode
     * @throws Exception
     */
    protected final void executeQueriesByBatchForDetail(List<QueryParamEntity> queries, Schema schema, JobInfo.WriteDisposition mode) throws Exception {

        //filter all days queries
        List<QueryParamEntity> daysList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.DAY).collect(Collectors.toList());
        if (!daysList.isEmpty()) {
            LOGGER.debug("daysList query size is {}", daysList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(daysList, schema, mode, PeriodType.DAY);
        }

        //filter all week queries
        List<QueryParamEntity> weekList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.WEEK).collect(Collectors.toList());
        if (!weekList.isEmpty()) {
            LOGGER.debug("weekList query size is {}", weekList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(weekList, schema, mode, PeriodType.WEEK);
        }

        //filter all month queries
        List<QueryParamEntity> monthList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.MONTH).collect(Collectors.toList());
        if (!monthList.isEmpty()) {
            LOGGER.debug("monthList query size is {}", monthList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(monthList, schema, mode, PeriodType.MONTH);
        }

        //filter all RTD queries
        List<QueryParamEntity> rtdList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.RTD).collect(Collectors.toList());
        if (!rtdList.isEmpty()) {
            LOGGER.debug("rtdList query size is {}", rtdList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(rtdList, schema, mode, PeriodType.RTD);
        }


    }


    /**
     * Execute Queries by batches for top performer module, splitting them based on interavals, starting from  week,month, quarter, year
     *
     * @param queries
     * @param schema
     * @param mode
     * @throws Exception
     */
    protected final void executeQueriesByBatchForTopPerformers(List<QueryParamEntity> queries, Schema schema, JobInfo.WriteDisposition mode) throws Exception {

        //filter all week & month queries
        List<QueryParamEntity> weekMonthList = queries.stream().filter(paramEntity -> (paramEntity.getPeriodType() == PeriodType.WEEK || paramEntity.getPeriodType() == PeriodType.MONTH)).collect(Collectors.toList());
        if (!weekMonthList.isEmpty()) {
            LOGGER.debug("weekMonthList query size is {}", weekMonthList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(weekMonthList, schema, mode, null);
        }

        //filter all quarter queries
        List<QueryParamEntity> quarterList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.QUARTER).collect(Collectors.toList());
        if (!quarterList.isEmpty()) {
            LOGGER.debug("quarterList query size is {}", quarterList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(quarterList, schema, mode, PeriodType.QUARTER);
        }

        //filter all year queries
        List<QueryParamEntity> yearList = queries.stream().filter(paramEntity -> paramEntity.getPeriodType() == PeriodType.YEAR).collect(Collectors.toList());
        if (!yearList.isEmpty()) {
            LOGGER.debug("yearList query size is {}", yearList.get(0).getQueryMap().size());
            List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(yearList, schema, mode, PeriodType.YEAR);
        }
    }

    /**
     * Execute all Queries by batches
     *
     * @param queries
     * @param schema
     * @param mode
     * @throws Exception
     */
    protected final void executeALLQueriesPeriodTypes(List<QueryParamEntity> queries, Schema schema, JobInfo.WriteDisposition mode) throws Exception {
        List<Map<String, Boolean>> results = BigQueryLoadAPIUtil.executeAsynchronousQueries(queries, schema, mode, null);
    }


    /**
     * load All Agg data to BT as single job
     *
     * @param options
     * @param dataLoadingSchema
     * @param lookUp
     * @param queries
     * @throws Exception
     */
    protected void loadAggregationbyCombinedPcoll(SSTSpeedLayerOptions options, Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema,
                                                  Map<String, Map<String, List<String>>> lookUp, List<QueryParamEntity> queries) throws Exception {
        // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
        CloudBigtableTableConfiguration config =
                CloudBigtableTableConfiguration.fromCBTOptions(options);

        final Pipeline pipeline = Pipeline.create(options);

        PCollectionList<TableRow> aggPcollectionList = null;

        String btTableName = "";

        //for each query issue DF job
        for (QueryParamEntity entity : queries) {


            if (StringUtils.isEmpty(btTableName)) {
                btTableName = entity.getDestinationTable();
            }

            LOGGER.debug("sql is {} " + entity.getQuery());

            PCollection<TableRow> aggPcollection = pipeline.apply(BigQueryIO.Read
                    .named(entity.toStringShard() + (StringUtils.isNotEmpty(entity.getPartitionLoad()) ? ":" + entity.getPartitionLoad() : ""))
                    .fromQuery(entity.getQuery()).usingStandardSql());

            aggPcollectionList = aggPcollectionList == null ? PCollectionList.of(aggPcollection) : aggPcollectionList.and(aggPcollection);

        }

        //flatten pcollectionlist
        PCollection<TableRow> mergedPcollection = aggPcollectionList.apply(Flatten.<TableRow>pCollections());

        // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them
        // through the network
        CloudBigtableIO.initializeForWrite(pipeline);

        CloudBigtableScanConfiguration bconfig = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(StringUtils.isEmpty(options.getBigtableTableId()) ? btTableName : options.getBigtableTableId())
                .build();

        //create mutation pcollection
        PCollection inputCollectionMutated = (PCollection) mergedPcollection
                .apply(ParDo.of(new TableRowToPutMutationParser(dataLoadingSchema, lookUp)));


        inputCollectionMutated.apply(CloudBigtableIO.writeToTable(config));

        pipeline.run();

    }

    /**
     * delete Aggreagtion data from BT
     *
     * @param options
     * @param dataLoadingSchema
     * @param queries
     * @throws Exception
     */
    protected void removeAggregationData(SSTSpeedLayerOptions options, Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema,
                                         List<QueryParamEntity> queries) throws Exception {
        // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
        CloudBigtableTableConfiguration config =
                CloudBigtableTableConfiguration.fromCBTOptions(options);

        final Pipeline pipeline = Pipeline.create(options);

        PCollectionList<TableRow> removePcollectionList = null;

        //for each query issue DF job
        for (QueryParamEntity entity : queries) {

            LOGGER.debug("sql is {} " + entity.getQuery());
            PCollection<TableRow> rPcollection = pipeline.apply(BigQueryIO.Read
                    .named("Delete query running in shard " + entity.toStringShard())
                    .fromQuery(entity.getQuery()).usingStandardSql());

            removePcollectionList = removePcollectionList == null ? PCollectionList.of(rPcollection) : removePcollectionList.and(rPcollection);

        }

        //flatten pcollectionlist
        PCollection<TableRow> mergedRemovePcollection = removePcollectionList.apply(Flatten.<TableRow>pCollections());

        // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them
        // through the network
        CloudBigtableIO.initializeForWrite(pipeline);

        CloudBigtableScanConfiguration bconfig = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();

        //create mutation pcollection
        PCollection inputCollectionMutated = (PCollection) mergedRemovePcollection
                .apply(ParDo.of(new TableRowToDeleteMutationParser(dataLoadingSchema)));


        inputCollectionMutated.apply(CloudBigtableIO.writeToTable(config));

        pipeline.run();


    }
}
