package com.umusic.gcp.sst.speedlayer.data.util;

import com.google.cloud.bigquery.*;
import com.umusic.gcp.sst.speedlayer.data.entity.PartitionEntity;
import com.umusic.gcp.sst.speedlayer.data.entity.QueryParamEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import com.umusic.gcp.sst.speedlayer.data.exception.SSTSpeedLayerException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by arumugv on 3/29/17.
 * BigQuery Utility class
 */
public class BigQueryLoadAPIUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(BigQueryLoadAPIUtil.class);

    //bq instance
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();


    /**
     * Check BQ table exists
     *
     * @param datasetId
     * @param tableName
     * @return
     */
    public static boolean checkTableExists(String datasetId, String tableName) {
        Table table = bigquery.getTable(datasetId, tableName);
        if (table == null) {
            return false;
        }
        return true;
    }

    /**
     * Creates BQ partition table
     *
     * @param project
     * @param dataset
     * @param tableName
     * @param schema
     */
    public static void createPartitionTable(String project, String dataset, String tableName,
                                            Schema schema) {
        TableId tableId = null;
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataset, tableName);
        } else {
            tableId = TableId.of(dataset, tableName);
        }


        //TableDefinition tableDefinition =
        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))

                .setSchema(schema);
        TableDefinition tableDefinition = builder.build();
        //tableDefinition.
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
    }


    /**
     * Load period lookup fron Big query
     *
     * @param startDate
     * @param endDate
     * @param type
     * @return
     */
    public static Map<Long, PartitionEntity> loadPeriodLookup(String sql, String startDate, String endDate, PeriodType type) throws Exception {
        Map<Long, PartitionEntity> lookUpPeriodMap = new HashMap<Long, PartitionEntity>();

        boolean isCompleted = false;

        sql = sql.replace("${START_DATE}", startDate).replace("${END_DATE}", endDate);
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();
        LOGGER.debug("Look up sql is {}", sql);
        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing lookup query " + response.getExecutionErrors());
        }


        QueryResult result = response.getResult();
        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                if (!lookUpPeriodMap.containsKey(row.get(0).getLongValue())) {
                    PartitionEntity partitionEntity = new PartitionEntity();
                    partitionEntity.setPeriod(row.get(0).getLongValue());
                    partitionEntity.setPeriodType(type);
                    partitionEntity.setPartitionStartDate(row.get(1).getStringValue());
                    partitionEntity.setPartitionEndDate(row.get(2).getStringValue());
                    partitionEntity.setPartitionPrevStartDate(row.get(3).getStringValue());
                    partitionEntity.setPartitionPrevEndDate(row.get(4).getStringValue());
                    lookUpPeriodMap.put(row.get(0).getLongValue(), partitionEntity);
                }

            }
            result = result.getNextPage();
        }
        LOGGER.debug("lookUpPeriodMap is {}", lookUpPeriodMap.size());
        return lookUpPeriodMap;

    }


    /**
     * Find max transaction
     *
     * @param sql
     * @return
     */
    public static String findMaxTransactionDate(String sql) throws Exception {
        // List<ChangeSet> listChangeset = new ArrayList<ChangeSet>();
        String maxTransactionDate = "";
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing max transaction date query " + response.getExecutionErrors());
        }
        QueryResult result = response.getResult();

        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                maxTransactionDate = row.get(0).getStringValue();


            }
            result = result.getNextPage();
        }

        return maxTransactionDate;

    }

    /**
     * find impacted report date
     *
     * @param sql
     * @return
     * @throws Exception
     */
    public static Map<String, String> findReportDates(String sql) throws Exception {

        Map<String, String> reportDateMap = new HashMap<String, String>();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing report date query " + response.getExecutionErrors());
        }
        QueryResult result = response.getResult();

        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                reportDateMap.put("report_start_date", row.get(0).getStringValue());
                reportDateMap.put("report_end_date", row.get(1).getStringValue());


            }
            result = result.getNextPage();
        }

        return reportDateMap;

    }


    /**
     * find changeset count
     *
     * @param sql
     * @return
     */
    public static long findChangesetCount(String sql) throws Exception {
        // List<ChangeSet> listChangeset = new ArrayList<ChangeSet>();
        long count = 0;
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing lookup query " + response.getExecutionErrors());
        }
        QueryResult result = response.getResult();

        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                count = row.get(0).getLongValue();


            }
            result = result.getNextPage();
        }

        return count;

    }


    /**
     * Load country region lookup from big query
     *
     * @return
     */
    public static Map<String, List<String>> loadCountryRegionMap(String sql) throws Exception {

        Map<String, List<String>> countryRegionMap = new HashMap<String, List<String>>();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing lookup query " + response.getExecutionErrors());
        }
        QueryResult result = response.getResult();
        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                if (countryRegionMap.containsKey(row.get(0).getStringValue())) {
                    countryRegionMap.get(row.get(0).getStringValue()).add(row.get(1).getStringValue());
                } else {
                    List<String> ls = new ArrayList<String>();
                    ls.add(row.get(1).getStringValue());
                    countryRegionMap.put(row.get(0).getStringValue(), ls);
                }

            }
            result = result.getNextPage();
        }

        return countryRegionMap;

    }


    /**
     * remove processed dates
     *
     * @return
     */
    public static boolean removeProcessedDates(String sql) throws Exception {

        Map<String, List<String>> countryRegionMap = new HashMap<String, List<String>>();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(sql)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse response = bigquery.query(queryRequest);
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            throw new SSTSpeedLayerException("Error occured while executing delete operation on processed dates " + response.getExecutionErrors());
        }


        return true;

    }

    /**
     * Executes asynchoronous queries and load data on BQ, creates table if doesn't exist
     *
     * @param querieList
     * @param schema
     * @return
     * @throws Exception
     */
    public static List<Map<String, Boolean>> executeAsynchronousQueries(List<QueryParamEntity> querieList, Schema schema, JobInfo.WriteDisposition mode, PeriodType period) throws Exception {
        List<Map<String, Boolean>> listJobResult = new ArrayList<Map<String, Boolean>>();
        Map<String, Boolean> jobResult = new HashMap<String, Boolean>();
        Map<String, String> jobIDMapping = new HashMap<String, String>();


        for (QueryParamEntity entity : querieList) {
            TableId shard = null;
            String tableName = entity.getTable();
            String fullTable = "";
            //use shard code along with table name if provided
            if (StringUtils.isNotEmpty(String.valueOf(entity.getShard()))) {
                tableName = tableName + "_" + String.valueOf(entity.getShard());
            }

            if (entity.isPartitionTable() && !checkTableExists(entity.getDataset(), tableName) && schema != null) {
                createPartitionTable(entity.getProject(), entity.getDataset(), tableName, schema);
            }

            SortedMap<String, String> queryMap = entity.getQueryMap();
            //List<SortedMap<String, String>> listSplitQuerymap= SSTStageBuilder.splitMap(queryMap, 50);


            LOGGER.info("count of {} query list for shard {} is {}", entity.getPeriodType() != null ? entity.getPeriodType().name() : "", entity.getShard(), queryMap.size());

            // for(SortedMap<String, String> splitQueryMap :  listSplitQuerymap){
            // Map<String, Boolean> jobResult = new HashMap<String, Boolean>();
            // Map<String, String> jobIDMapping = new HashMap<String, String>();
            for (Map.Entry<String, String> entry : queryMap.entrySet()) {
                if (entity.isPartitionTable()) {
                    fullTable = tableName + "$" + entry.getKey();
                } else {
                    fullTable = tableName;
                }

                if (StringUtils.isNotEmpty(entity.getProject())) {
                    shard = TableId.of(entity.getProject(), entity.getDataset(), fullTable);
                } else {
                    shard = TableId.of(entity.getDataset(), fullTable);
                }
                //  Table test = new Table();

                //String query = querieMap.get(entity);
                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(entry.getValue())
                                .setDestinationTable(shard)
                                .setUseLegacySql(false)
                                .setFlattenResults(true)
                                .setAllowLargeResults(true)
                                .setPriority(QueryJobConfiguration.Priority.BATCH)
                                //.setUseQueryCache(false)
                                .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                                .setWriteDisposition(mode)
                                //     JobInfo.WriteDisposition.WRITE_TRUNCATE)
                                .build();
                String uuid = UUID.randomUUID().toString();
                JobId jobId = JobId.of(uuid);
                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
                LOGGER.debug(" job created for shard {} query is \n {}", fullTable, entry.getValue());
                jobIDMapping.put(fullTable, uuid);
            }


            // }


        }

        while (true) {
            boolean allDone = true; //not really, but if we found at least one still running we'll update
            boolean anyErrors = false; // starting no errors, any error pops up set it to false
            for (String fullName : jobIDMapping.keySet()) {

                Job queryJob = bigquery.getJob(jobIDMapping.get(fullName));
                JobStatus status = queryJob.getStatus();
                //LOGGER.info("Query job shard is {} status is {} ", entity.toStringShard() , status.getState().toString());
                if (status.getState() == JobStatus.State.RUNNING) {
                    LOGGER.debug("query job shard  -> {} status is {}", fullName, status.getState().toString());
                    allDone = false;
                } else if (status.getState() == JobStatus.State.PENDING) {
                    LOGGER.debug("query job shard  -> {} status is {}", fullName, status.getState().toString());
                    allDone = false;
                } else if (status.getState() == JobStatus.State.DONE &&
                        (status.getExecutionErrors() == null ||
                                status.getExecutionErrors().isEmpty())) {
                    jobResult.put(fullName, true);
                    LOGGER.debug("query job shard  -> {} status is done", fullName);

                } else if (status.getState() == JobStatus.State.DONE &&
                        !status.getExecutionErrors().isEmpty()) {
                    LOGGER.error("query job -> {} is failed, and error is {} ", fullName, status.getExecutionErrors());
                    anyErrors = true;
                    jobResult.put(fullName, false);

                } else {
                    allDone = false;
                }

            }

            if (allDone && anyErrors) {
                LOGGER.error("{} query jobs are partially done, there are some errors. cancelling further execution", period.name());
                throw new SSTSpeedLayerException("Query Job failed ");
            } else if (allDone) {
                LOGGER.info("{} query jobs are done", period != null ? period.name() : "");
                break;
            }
            Thread.sleep(5000);
        }
        listJobResult.add(jobResult);


        return listJobResult;
    }


}

