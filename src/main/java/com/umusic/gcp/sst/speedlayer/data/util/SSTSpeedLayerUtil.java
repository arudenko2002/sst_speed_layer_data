package com.umusic.gcp.sst.speedlayer.data.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.bigquery.Field;
import com.umusic.gcp.sst.speedlayer.data.batch.*;
import com.umusic.gcp.sst.speedlayer.data.entity.*;
import com.umusic.gcp.sst.speedlayer.data.enumtype.ChangeSetType;
import com.umusic.gcp.sst.speedlayer.data.enumtype.Module;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import com.umusic.gcp.sst.speedlayer.data.enumtype.Stage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by arumugv on 3/27/17.
 * Utility class for SST Speed layer
 */
public class SSTSpeedLayerUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerUtil.class);

    //collection to store look up for different periods
    private static Map<Long, PartitionEntity> dayMap = new HashMap<Long, PartitionEntity>();
    private static Map<Long, PartitionEntity> weekMap = new HashMap<Long, PartitionEntity>();
    private static Map<Long, PartitionEntity> quarterMap = new HashMap<Long, PartitionEntity>();
    private static Map<Long, PartitionEntity> monthMap = new HashMap<Long, PartitionEntity>();
    private static Map<Long, PartitionEntity> yearMap = new HashMap<Long, PartitionEntity>();
    private static Map<Long, PartitionEntity> rtdMap = new HashMap<Long, PartitionEntity>();

    private static DateTimeFormatter dateWithoutDash = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static DateTimeFormatter dateFormatWithDash = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static SSTConfiguration config;


    static{
        try {
            config = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.SST_CONFIGURATION_FILENAME), SSTConfiguration.class);
        }catch(IOException e){
            throw new ExceptionInInitializerError(e);
        }
    }
    /**
     * Based on stage/functionality returns the correspoding generator instance
     * @param stage
     * @return
     * @throws Exception
     */
    public static GenericSSTSpeedLayerGenerator findStage(String stage) throws Exception {
        Stage stageEnum = Stage.valueOf(stage);
        switch (stageEnum){
            case BUILD:
                return new SSTSpeedLayerAggregationBuilder();
            case LOAD:
                return new SSTSpeedLayerAggregationLoader();
            case CHANGESETBUILD:
                return new SSTSpeedLayerAggChangesetBuilder();
            case CHANGESETLOAD:
                return new SSTSpeedLayerAggChangesetLoader();
            case CHANGESETUNLOAD:
                return new SSTSpeedLayerAggChangesetEraser();
            //This is stage is not used anymore, removing the processed(scheduled) dates using sql in airflow job
            //case REMOVEPROCESSEDDATE:
            //    return new SSTSpeedLayerProcessedDateEraser();
            default:
                return null;
        }

    }

    /**
     * replace dataset template varaible
     * @param sql
     * @return
     */
    public static String replaceDatasetTableVariables(String sql, String productDate){
        Map<String, String> datsetMap = config.getDatasetName();
        Map<String, String> tableMap = config.getTableName();
        Map<String, String> tableAlias = config.getTableAlias();

        for(String key : datsetMap.keySet()){
            if(sql.contains(key))
                sql =  sql.replace(key, datsetMap.get(key));
        }

        for(String key : tableMap.keySet()){
            if(sql.contains(key))
            sql =  sql.replace(key, tableMap.get(key));
        }

        for(String key : tableAlias.keySet()){
            if(sql.contains(key))
             sql =  sql.replace(key, tableAlias.get(key));
        }

        if(StringUtils.isNotEmpty(productDate)){
            LocalDate loadDateObj = LocalDate.parse(productDate, dateFormatWithDash);
            if(sql.contains("${PRODUCT_LOAD_DATE}")){
                sql =  sql.replace("${PRODUCT_LOAD_DATE}", loadDateObj.format(dateWithoutDash));
            }
        }


        LOGGER.debug("sql after replaced is {} ", sql);

        return sql;
    }

    /**
     * find dataset name from yaml config file
     * @param var
     * @return
     */
    public static String findDatasetName(String var) {
        Map<String, String> datsetMap = config.getDatasetName();
        if(datsetMap.containsKey(var)){
            return datsetMap.get(var);
        }
        return var;
    }

    /**
     * find table name from yaml config file
     * @param var
     * @return
     */
    public static String findTableName(String var) {
        Map<String, String> tableMap = config.getTableName();
        if(tableMap.containsKey(var)){
            return tableMap.get(var);
        }
        return var;
    }

    /**
     * find queries from  yaml config file
     * @param queryType
     * @param queryName
     * @return
     */
    public static String findQueriesFromConfig(String queryType, String queryName){
        LOGGER.debug("the queryType is  {} queryName is {} ", queryType, queryName);
        String sql = config.getQueries().get(queryType).get(queryName);
        //replace dataset,table variable
        sql = replaceDatasetTableVariables(sql, null);

        LOGGER.debug("the transformed {} sql is {} ", queryType, sql);

        return sql;
    }

    /**
     * find lookup query
     * @param queryName
     * @return
     */
    public static String findLookupquery(String queryName){
        return findQueriesFromConfig("lookup", queryName);
    }

    /**
     * find changeset query
     * @param queryName
     * @return
     */
    public static String findchangesetquery(String queryName){
        return findQueriesFromConfig("changeset", queryName);
    }


    /**
     * check dates with global week range
     * @param moduleStr
     * @param eltStart
     * @return
     */
    public static boolean isDatesWithinGlobalWeekRange(String moduleStr, String eltStart){

        Module module = Module.valueOf(moduleStr);

        //SSTConfiguration.WeekOrDayShard obj = config.getWeekOrDayShard();

        //its applicable for only detail pages, so if module not there always true
        if(!config.getWeekOrDayShard().getModules().contains(module)){
            return true;
        }

        //if WeekForPriorPeriod feature is not turned off, then false
        if(!config.isWeekForPriorPeriod()){
            return false;
        }

       // LocalDate weekStartDate = LocalDate.parse(config.getWeekOrDayShard().getWeekStartDate(), dateFormatWithDash);
        LocalDate weekEndDate = LocalDate.parse(config.getWeekOrDayShard().getWeekEndDate(), dateFormatWithDash);

        LocalDate eltStartDate = LocalDate.parse(eltStart, dateWithoutDash);

        //date falls after week global end date
        if(eltStartDate.isAfter(weekEndDate)){
            return false;
        }

        return true;
    }

    /**
     * check whether dates within global day range
     * @param moduleStr
     * @param eltEnd
     * @return
     */
    public static boolean isDatesWithinGlobalDayRange(String moduleStr,  String eltEnd){

        Module module = Module.valueOf(moduleStr);

        //its applicable for only detail pages, so if module not there always true
        if(!config.getWeekOrDayShard().getModules().contains(module)){
            return true;
        }


        //if WeekForPriorPeriod feature is not turned off, then days are always true
        if(!config.isWeekForPriorPeriod() ){
            return true;
        }

        // LocalDate weekStartDate = LocalDate.parse(config.getWeekOrDayShard().getWeekStartDate(), dateFormatWithDash);
        LocalDate daySTartDate = LocalDate.parse(config.getWeekOrDayShard().getDayStartDate(), dateFormatWithDash);
        LOGGER.debug("the eltEnd is {} ", eltEnd);
        LocalDate eltEndDate = LocalDate.parse(eltEnd, dateWithoutDash);

        if(eltEndDate.isBefore(daySTartDate)){
            return false;
        }

        return true;
    }

    /**
     * modify eltenddatefor building week agg
     * @param endDateStr
     * @param moduleStr
     * @return
     */
    public static String modifyEltEndDateForWeekAgg(String endDateStr, String moduleStr){
        Module module = Module.valueOf(moduleStr);
        //don't alter end date , if module is not in collection

        if(!config.getWeekOrDayShard().getModules().contains(module)){
            return endDateStr;
        }
        LocalDate weekEndDate = LocalDate.parse(config.getWeekOrDayShard().getWeekEndDate(), dateFormatWithDash);
        LocalDate eltEndDate = LocalDate.parse(endDateStr, dateWithoutDash);

        if(eltEndDate.isAfter(weekEndDate)){
            return weekEndDate.format(dateWithoutDash);
        }
        return endDateStr;
    }

    /**
     * modify eltstart date from building day agg
     * @param startDate
     * @param moduleStr
     * @return
     */
    public static String modifyEltStartDateForDayAgg(String startDate, String moduleStr){

        Module module = Module.valueOf(moduleStr);

        //don't alter end date , if global weekly for prior period is turned off
        if(!config.isWeekForPriorPeriod()){
            return startDate;
        }

        //don't alter end date , if module is not in collection
        if(!config.getWeekOrDayShard().getModules().contains(module)){
            return startDate;
        }
        LocalDate dayGlobalSTartDate = LocalDate.parse(config.getWeekOrDayShard().getDayStartDate(), dateFormatWithDash);
        LocalDate eltStartDate = LocalDate.parse(startDate, dateWithoutDash);


        if(eltStartDate.isBefore(dayGlobalSTartDate)){
            return dayGlobalSTartDate.format(dateWithoutDash);
        }
        return startDate;
    }

    /**
     * validate month can be build from days
     * @param partitionStartDate
     * @param moduleStr
     * @param comb
     * @param parentSource
     * @return
     */
    public static boolean validateMonthCanbeBuildFromDays(String partitionStartDate, String moduleStr, Combination comb, ParentShardSource parentSource){
        Module module = Module.valueOf(moduleStr);

        if(config.getWeekOrDayShard().getModules().contains(module) && config.isWeekForPriorPeriod()
                && comb.getPeriod().equals(PeriodType.MONTH) && parentSource.getPeriod().equals(PeriodType.DAY)){

            LocalDate patitionStartDt = LocalDate.parse(partitionStartDate, dateFormatWithDash);
            LocalDate dayGlobalSTartDate = LocalDate.parse(config.getWeekOrDayShard().getDayStartDate(), dateFormatWithDash);
            if(patitionStartDt.isEqual(dayGlobalSTartDate) || patitionStartDt.isAfter(dayGlobalSTartDate)){
                return true;
            }
            return false;
        }

        return true;
    }
    /**
     * Based on start and end date and type of period builds the collection with parttions
     * Queries the day table in Bigquery to get information.
     * @param comb
     * @param startDate
     * @param endDate
     * @return
     */
    public static Map<Long, PartitionEntity> partitionDateBuilder(Combination comb, String startDate, String endDate, String moduleStr, boolean isDynamicDates) throws Exception{
        switch(comb.getPeriod()){
            case DAY:
                if(dayMap == null || dayMap.isEmpty()) {
                    //LocalDate start = LocalDate.parse(startDate, dateWithoutDash);
                    //LocalDate end = LocalDate.parse(endDate, dateWithoutDash);
                    if(isDynamicDates){

                        String sql = findLookupquery(config.isDecisionEngineEnabled() ? SSTConsumptionConstants.SST_CONFIG_LKUP_DAY_DISTINCT_DYNAMICENGINE : SSTConsumptionConstants.SST_CONFIG_LKUP_DAY_DISTINCT );
                        dayMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.DAY);
                    }else{

                        String sql = findLookupquery(SSTConsumptionConstants.SST_CONFIG_LKUP_DAY_RANGE );
                        dayMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.DAY);
                    }
                }
                return dayMap;
            case WEEK:
                if(weekMap == null || weekMap.isEmpty()){

                    if(isDynamicDates){
                        String sql = findLookupquery(config.isDecisionEngineEnabled() ? SSTConsumptionConstants.SST_CONFIG_LKUP_WEEK_DISTINCT_DYNAMICENGINE : SSTConsumptionConstants.SST_CONFIG_LKUP_WEEK_DISTINCT);
                        weekMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.WEEK);
                    }else{

                        String sql = findLookupquery(SSTConsumptionConstants.SST_CONFIG_LKUP_WEEK_RANGE);
                        weekMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.WEEK);
                    }

                }
                return weekMap;
            case MONTH:
                if(monthMap == null || monthMap.isEmpty()){
                    String sql = "";
                    if(isDynamicDates){
                        sql = findLookupquery( config.isDecisionEngineEnabled() ? SSTConsumptionConstants.SST_CONFIG_LKUP_MONTH_DISTINCT_DYNAMICENGINE : SSTConsumptionConstants.SST_CONFIG_LKUP_MONTH_DISTINCT);
                    }else{
                        sql = findLookupquery( SSTConsumptionConstants.SST_CONFIG_LKUP_MONTH_RANGE);
                    }

                    monthMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.MONTH);
                }
                return monthMap;
            case QUARTER:
                if(quarterMap == null || quarterMap.isEmpty()){
                    String sql = "";
                    if(isDynamicDates){
                        sql = findLookupquery( config.isDecisionEngineEnabled() ? SSTConsumptionConstants.SST_CONFIG_LKUP_QUARTER_DISTINCT_DYNAMICENGINE : SSTConsumptionConstants.SST_CONFIG_LKUP_QUARTER_DISTINCT);
                    }else{
                        sql = findLookupquery( SSTConsumptionConstants.SST_CONFIG_LKUP_QUARTER_RANGE);
                    }
                    quarterMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.QUARTER);
                }
                return quarterMap;
            case YEAR:
                if(yearMap == null || yearMap.isEmpty()){
                    String sql = "";
                    if(isDynamicDates){
                        sql = findLookupquery( config.isDecisionEngineEnabled() ? SSTConsumptionConstants.SST_CONFIG_LKUP_YEAR_DISTINCT_DYNAMICENGINE : SSTConsumptionConstants.SST_CONFIG_LKUP_YEAR_DISTINCT);
                    }else{
                        sql = findLookupquery( SSTConsumptionConstants.SST_CONFIG_LKUP_YEAR_RANGE);
                    }

                    yearMap = BigQueryLoadAPIUtil.loadPeriodLookup(sql, startDate, endDate, PeriodType.YEAR);
                }
                return yearMap;
            case RTD:
                if(rtdMap == null || rtdMap.isEmpty()){
                    //current date
                    //Long longEndDate = Long.valueOf(LocalDate.now().format(dateWithoutDash));
                    PartitionEntity entity = new PartitionEntity();
                    entity.setPeriodType(PeriodType.RTD);
                    entity.setPartitionStartDate(config.getRtdStartDate());
                    entity.setPartitionEndDate(config.getRtdEndDate());
                    entity.setPeriod(-1L);
                    rtdMap.put(-1L, entity);

                }
                return rtdMap;

        }
        return null;
    }

    /**
     * Build lookup for country region relationship
     * @return Map<String, Map<String, List<String>>>
     */
    public static  Map<String, Map<String, List<String>>> buildLookup() throws Exception{
        Map<String, Map<String, List<String>>> lookUp = new HashMap<String, Map<String, List<String>>>();
        String sql = findLookupquery( "COUNTRY-REGION");
        lookUp.put("country-region", BigQueryLoadAPIUtil.loadCountryRegionMap(sql));
        return lookUp;
    }


    /**
     * filter dimension
     * @param filterArr
     * @param dimension
     * @return
     */
    public static ChangeSetFilter filterDimension(ChangeSetFilter[] filterArr, Dimension dimension){
        for(ChangeSetFilter chngFilter: filterArr){

            //check filter combination, if it fails go to next filterArr in for loop

            //check teriitory matches , if not not right filter
            if(chngFilter.getCombination().getTerritory() != null && !chngFilter.getCombination().getTerritory().equals(dimension.getCombination().getTerritory())){
                continue;
            }

            ////check teriitory subdivision matches , if not not right filter
            if(chngFilter.getCombination().getTerritorysubdivision() != null && dimension.getCombination().getTerritorysubdivision() != null &&
                !chngFilter.getCombination().getTerritorysubdivision().equals(dimension.getCombination().getTerritorysubdivision())){
                 continue;

            }

            //check teriitory matches , if not not right filter
            if(chngFilter.getCombination().getPeriod() != null && !chngFilter.getCombination().getPeriod().equals(dimension.getCombination().getPeriod())){
                continue;
            }


            //cross this stage means all filter paarms satisfied
            return chngFilter;



        }
        //nothing matched so return null
        return null;

    }


    /**
     * Load changsetConf based on changeset name
     * @param changeset
     * @return
     * @throws Exception
     */
    public final static ChangeSetConf findChangeSetConf(String changeset) throws Exception {
        ChangeSetType chngEnum = ChangeSetType.valueOf(changeset);
        switch (chngEnum){
            case TRACK_DAILY: case TRACK_WEEKLY:
                return mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_CHANGESET_CONFIG_FILE), ChangeSetConf.class);
            case PROJECT_DAILY: case PROJECT_WEEKLY:
                return mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_CHANGESET_CONFIG_FILE), ChangeSetConf.class);
            case ARTIST_DAILY: case ARTIST_WEEKLY:
                return mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_CHANGESET_CONFIG_FILE), ChangeSetConf.class);
            case REGION:
                return mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.REGION_CHANGESET_CONFIG_FILE), ChangeSetConf.class);
            default:
                return null;
        }

    }



    /**
     * Load changeSet finding SQL
     * @param changeset
     * @return
     * @throws Exception
     */
    public  final static String loadChangeSetSql(String changeset, String startDate, String endDate)  {
        String sql = config.getQueries().get("changeset").get(changeset);
        sql = sql.replace("${START_DATE}", startDate).replace("${END_DATE}", endDate);
        return sql;


    }

    /**
     * Queries and loads the list of changeset
     * @param changeset
     * @return
     */
    public final static long findChangesetCount(String changeset, String startDate, String endDate)throws Exception{
        String sql = findQueriesFromConfig("changeset-count", changeset);
        sql = sql.replace("${START_DATE}", startDate).replace("${END_DATE}", endDate);
        return BigQueryLoadAPIUtil.findChangesetCount(sql);
    }


    /**
     * Queries to find the dynamic report dates
     * @param startDate
     * @param endDate
     * @return
     * @throws Exception
     */
    public final static Map<String, String> findReportDates(String startDate, String endDate)throws Exception{
        String sql = findLookupquery("REPORT-DATES");
        sql = sql.replace("${START_DATE}", startDate).replace("${END_DATE}", endDate);
        return BigQueryLoadAPIUtil.findReportDates(sql);
    }


    /**
     * Builds BQ Field type, to build schema
     * @param type
     * @return
     */
    public static Field.Type getTypes(String type){
       if(type.equalsIgnoreCase("STRING")){
           return Field.Type.string();
       }else if(type.equalsIgnoreCase("INTEGER")){
           return Field.Type.integer();
       }else if(type.equalsIgnoreCase("FLOAT")){
           return Field.Type.floatingPoint();
       }else if(type.equalsIgnoreCase("TIMESTAMP")){
           return Field.Type.timestamp();
       }
       return null;
    }

    /**
     * Splits the map into multiple maps based on size of split.
     * @param map
     * @param size
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> List<SortedMap<K, V>> splitMap(final SortedMap<K, V> map, final int size) {
        List<K> keys = new ArrayList<>(map.keySet());
        List<SortedMap<K, V>> parts = new ArrayList<>();
        final int listSize = map.size();
        for (int i = 0; i < listSize; i += size) {
            if (i + size < listSize) {
                parts.add(map.subMap(keys.get(i), keys.get(i + size)));
            } else {
                parts.add(map.tailMap(keys.get(i)));
            }
        }
        return parts;
    }

    /**
     * backoff in case of failure
     * @param sec
     */
    public static void backoff(int sec) {
        try {
            double delay = Math.pow(2, (double)sec);
            Thread.sleep((long)delay + 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to write all sql queries generated to write to file.
     * @param module
     * @param queries
     */
    public static void writeQueriesToFile(String module, List<QueryParamEntity> queries){
        for (QueryParamEntity entity : queries){
            for(String date :  entity.getQueryMap().keySet()){
                try {
                    FileOutputStream fos = FileUtils.openOutputStream(new File("/tmp/sst-queries/" + module + "/" + entity.getShard() + "/" + entity.getShard() + "_" + date + ".txt"), true);


                           // new FileOutputStream("/tmp/" + module + "/" + entity.getShard() + "/" + entity.getShard() + "_" + date + ".txt");
                    Writer oos = new OutputStreamWriter(fos);
                    oos.write(entity.getQueryMap().get(date));
                    oos.close();
                    fos.close();
                }catch(IOException ioe) {
                    ioe.printStackTrace();
                }
            }

        }



    }

    /**
     * remove processed(scheduled) dates
     * @throws Exception
     */
    public static void removeProcessedDates() throws Exception{
        String sql = findQueriesFromConfig("data-availability", "REMOVE-PROCESSED-DATES");
        BigQueryLoadAPIUtil.removeProcessedDates(sql);
    }


    /**
     * check dynamic date turned on
     * @param dynamicMode
     * @return
     */
    public static boolean isDynamicMode(String dynamicMode){
        if(!StringUtils.isEmpty(dynamicMode) && dynamicMode.equalsIgnoreCase("Y")){
            return true;
        }

        return false;
    }

    /**
     * check is it top performer module
     * @param module
     * @return
     */
    public static boolean isTopPerformer(String module){
        Module mod = Module.valueOf(module);
        if(mod.equals(Module.TOPARTIST) || mod.equals(Module.TOPPROJECT) || mod.equals(Module.TOPTRACK)){
            return true;
        }
        return false;
    }

    /**
     * check if it is deltaAgg mode
     * @param deltaAggMode
     * @return
     */
    public static boolean isDeltaAggregationMode(String deltaAggMode){
        if(!StringUtils.isEmpty(deltaAggMode) && deltaAggMode.equalsIgnoreCase("Y")){
            return true;
        }

        return false;
    }

    /**
     * check cross module turned on
     * @return
     */
    public static boolean isCrossModuleBuild(){
        return config.isCrossModuleBuild();
    }

}
