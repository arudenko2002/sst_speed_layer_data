package com.umusic.gcp.sst.speedlayer.data.queryengine;

import com.umusic.gcp.sst.speedlayer.data.batch.SSTSpeedLayerAggregationLoader;
import com.umusic.gcp.sst.speedlayer.data.entity.*;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.enumtype.Module;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import com.umusic.gcp.sst.speedlayer.data.util.SSTConsumptionConstants;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by arumugv on 3/23/17.
 * class to generate queries while loading to big table
 */
public class SSTAggregationLoadQueryEngine extends SSTQueryEngine {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerAggregationLoader.class);


    /**
     * constructor for regular load
     * @param module
     * @param eltStartDate
     * @param eltEndDate
     * @param loadDate
     * @param productDate
     * @param dynamicDateMode
     * @param deltaAggMode
     * @throws Exception
     */
    public SSTAggregationLoadQueryEngine(String module, String eltStartDate, String eltEndDate, String loadDate, String productDate, String dynamicDateMode, String deltaAggMode) throws Exception {
        super(module, eltStartDate, eltEndDate, loadDate, productDate, dynamicDateMode, deltaAggMode);
    }

    /**
     * consutructor for changeset load/unload
     * @param module
     * @param loadDate
     * @param eltStartDate
     * @param eltEndDate
     * @param changeSet
     * @param chngStartDate
     * @param chngEndDate
     * @param conf
     * @param productDate
     * @throws Exception
     */
    public SSTAggregationLoadQueryEngine(String module, String loadDate, String eltStartDate, String eltEndDate,
                                          String changeSet, String chngStartDate, String chngEndDate, ChangeSetConf conf, String productDate) throws Exception{
        super(module, eltStartDate, eltEndDate, loadDate, changeSet, chngStartDate, chngEndDate,  conf, productDate);
    }

    @Override
    protected  void loadDependentTableCollection() throws IOException {

    }

    /**
     * fetch query to extract keys from BQ to  unload from BT table
     * @throws Exception
     */
    @Override
    protected  void loadAlternateDeleteQuery() throws Exception{
        Module mod = Module.valueOf(module);
        switch (mod){
            case TRACK:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_UNLOAD_DELETE_SQL);
                break;
            case PROJECT:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_UNLOAD_DELETE_SQL);
                break;
            case ARTIST:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_UNLOAD_DELETE_SQL);
                break;
            default:
                return ;
        }
    }

    /**
     * load BT table schema
     * @throws IOException
     */
    @Override
    protected void loadDestinationSchema() throws IOException {
        Module module = Module.valueOf(this.module);

        switch (module) {
            case TOPARTIST:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case TOPPROJECT:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case TOPTRACK:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case ARTIST:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case PROJECT:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case TRACK:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case ISRC:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            case ALBUM:
                btTableSchema = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_BT_TABLE_SCHEMA), BTTableEntity.class);
                break;
            default:
                ;
        }
    }


    /**
     * load sql template
     * @throws IOException
     */
    @Override
    protected void loadSQLTemplate() throws IOException {
        Module module = Module.valueOf(this.module);
        switch (module){
            case TOPARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_STAGE2_SQLTEMPLATE);
                break;
            case TOPPROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_STAGE2_SQLTEMPLATE);
                break;
            case TOPTRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_STAGE2_SQLTEMPLATE);
                break;
            case ARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_STAGE2_SQLTEMPLATE);
                break;
            case PROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_STAGE2_SQLTEMPLATE);
                break;
            case TRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_STAGE2_SQLTEMPLATE);
                break;
            case ISRC:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_LOAD_SQLTEMPLATE);
                break;
            case ALBUM:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_LOAD_SQLTEMPLATE);
                break;
            default:
                ;
        }
    }


    /**
     * build module specific BT loading types
     * @param loadingTypes
     * @return
     */
    private Map<DataLoadingType, String> buildLoadingTypesSchema(DataLoadingType[] loadingTypes){
        Module module = Module.valueOf(this.module);

        Map<DataLoadingType, String> loadingschema = new HashMap<DataLoadingType, String>();
        switch (module){
            case TOPARTIST:
                loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.TOPARTIST_BIGTABLE_CONFIG);
                break;
            case TOPPROJECT:
                loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.TOPPROJECT_BIGTABLE_CONFIG);
                break;
            case TOPTRACK:
                loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.TOPTRACK_BIGTABLE_CONFIG);
                break;
            case ARTIST:
                if(loadingTypes != null && loadingTypes.length > 0) {
                    for (DataLoadingType loadingType : loadingTypes) {
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER, SSTConsumptionConstants.ARTIST_BIGTABLE_BYPARTNER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER_SPECIFIC_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER_SPECIFIC_PARTNER, SSTConsumptionConstants.ARTIST_BIGTABLE_BYPARTNER_SPECIFIC_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_ALL_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_ALL_REGION, SSTConsumptionConstants.ARTIST_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION, SSTConsumptionConstants.ARTIST_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY, SSTConsumptionConstants.ARTIST_BIGTABLE_BYCOUNTRY_CONFIG);
                        }

                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SUBDIVISION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SUBDIVISION, SSTConsumptionConstants.ARTIST_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG);
                        }

                        if (loadingType != null && loadingType.equals(DataLoadingType.DEFAULT)) {
                            loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.ARTIST_BIGTABLE_DEFAULT_CONFIG);
                        }

                    }
                }
                //tableschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.ARTIST_BIGTABLE_DEFAULT_CONFIG);
                break;
            case PROJECT:
                if(loadingTypes != null && loadingTypes.length > 0) {
                    for (DataLoadingType loadingType : loadingTypes) {
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER, SSTConsumptionConstants.PROJECT_BIGTABLE_BYPARTNER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER_SPECIFIC_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER_SPECIFIC_PARTNER, SSTConsumptionConstants.PROJECT_BIGTABLE_BYPARTNER_SPECIFIC_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_ALL_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_ALL_REGION, SSTConsumptionConstants.PROJECT_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION, SSTConsumptionConstants.PROJECT_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY, SSTConsumptionConstants.PROJECT_BIGTABLE_BYCOUNTRY_CONFIG);
                        }

                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SUBDIVISION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SUBDIVISION, SSTConsumptionConstants.PROJECT_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG);
                        }

                        if (loadingType != null && loadingType.equals(DataLoadingType.DEFAULT)) {
                            loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.PROJECT_BIGTABLE_DEFAULT_CONFIG);
                        }

                    }
                }

                break;
            case TRACK:
                if(loadingTypes != null && loadingTypes.length > 0) {
                    for (DataLoadingType loadingType : loadingTypes) {
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER, SSTConsumptionConstants.TRACK_BIGTABLE_BYPARTNER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_ALL_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_ALL_REGION, SSTConsumptionConstants.TRACK_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION, SSTConsumptionConstants.TRACK_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY, SSTConsumptionConstants.TRACK_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SUBDIVISION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SUBDIVISION, SSTConsumptionConstants.TRACK_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.DEFAULT)) {
                            loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.TRACK_BIGTABLE_DEFAULT_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_USAGETYPE_TIER_SPECIFIC_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_USAGETYPE_TIER_SPECIFIC_PARTNER, SSTConsumptionConstants.TRACK_BIGTABLE_USAGETYPE_TIER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_USAGETYPE_TIER_ALL_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_USAGETYPE_TIER_ALL_PARTNER, SSTConsumptionConstants.TRACK_BIGTABLE_PARTNER_USAGETYPE_TIER_CONFIG);
                        }
                    }
                }

                break;
            case ISRC:
                if(loadingTypes != null && loadingTypes.length > 0) {
                    for (DataLoadingType loadingType : loadingTypes) {
                        if (loadingType != null && loadingType.equals(DataLoadingType.DEFAULT)) {
                            loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.ISRC_LOADING_DEFAULT_MAPPING);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_PARTNER, SSTConsumptionConstants.ISRC_BIGTABLE_BYPARTNER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_ALL_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_ALL_REGION, SSTConsumptionConstants.ISRC_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION, SSTConsumptionConstants.ISRC_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY, SSTConsumptionConstants.ISRC_BIGTABLE_BYCOUNTRY_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_COUNTRY_SUBDIVISION)) {
                            loadingschema.put(DataLoadingType.BY_COUNTRY_SUBDIVISION, SSTConsumptionConstants.ISRC_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_USAGETYPE_TIER_SPECIFIC_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_USAGETYPE_TIER_SPECIFIC_PARTNER, SSTConsumptionConstants.ISRC_BIGTABLE_USAGETYPE_TIER_CONFIG);
                        }
                        if (loadingType != null && loadingType.equals(DataLoadingType.BY_USAGETYPE_TIER_ALL_PARTNER)) {
                            loadingschema.put(DataLoadingType.BY_USAGETYPE_TIER_ALL_PARTNER, SSTConsumptionConstants.ISRC_BIGTABLE_PARTNER_USAGETYPE_TIER_CONFIG);
                        }
                    }
                }

                break;
            case ALBUM:
                if(loadingTypes != null && loadingTypes.length > 0) {
                    for (DataLoadingType loadingType : loadingTypes) {
                        if (loadingType != null && loadingType.equals(DataLoadingType.DEFAULT)) {
                            loadingschema.put(DataLoadingType.DEFAULT, SSTConsumptionConstants.ALBUM_LOADING_DEFAULT_MAPPING);
                        }
                    }
                }
                break;
            default:

        }
        return loadingschema;
    }


    /**
     * genrate queries to load from start shard number to end shard number
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    @Override
    public List<QueryParamEntity> buildQuery(Integer start, Integer end) throws Exception {
        Dimension[] dimensionArr = shard.getDimension();
        String sourcetable = shard.getDataset() + "." + shard.getStagingTable();
        for (Dimension dimension : dimensionArr) {
            ChangeSetFilter chngStFilter = null;

            //check if start and end shard is provided and they current shard code is not within range, skip
            if (start != null && end != null && (dimension.getShardcode() < start || dimension.getShardcode() > end)) {
                continue;
            }
            //if action is changset & filter exist, filter out dimension based on changeset conf
            if (this.changeSetConf != null && changeSetConf.getFilterArr() != null && changeSetConf.getFilterArr().get("load") != null) {
                chngStFilter = SSTSpeedLayerUtil.filterDimension(changeSetConf.getFilterArr().get("load"), dimension);
                if (chngStFilter == null) {
                    continue;
                }
            }
            Combination comb = dimension.getCombination();

            String fullTableVar = "`" + sourcetable + "_" + dimension.getShardcode() + "`";

            String startDate = eltStartDate;
            String endDate = eltEndDate;

            Map<Long, PartitionEntity> totalPartitions = SSTSpeedLayerUtil.partitionDateBuilder(dimension.getCombination(), startDate, endDate, this.module, SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode));

            //checks the dates are legitimate for the global week or day dimension specified for specific modules
            if (!SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)) {

                if (comb.getPeriod().equals(PeriodType.WEEK) && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, this.eltStartDate)) {
                    continue;
                } else if (comb.getPeriod().equals(PeriodType.DAY) && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, this.eltEndDate)) {
                    continue;
                }

                if (comb.getPeriod().equals(PeriodType.DAY)) {
                    //Modify elt start date if it is after global range for detail pages
                    startDate = SSTSpeedLayerUtil.modifyEltStartDateForDayAgg(startDate, this.module);
                }

                if (comb.getPeriod().equals(PeriodType.WEEK)) {
                    //Modify elt end date if it is after global range for detail modules
                    endDate = SSTSpeedLayerUtil.modifyEltEndDateForWeekAgg(endDate, this.module);
                }


                List<Long> keyList = new ArrayList<Long>(totalPartitions.keySet());

                Collections.sort(keyList);

                String partitonStr = "";

                StringBuffer buff = new StringBuffer();
                if (dimension.getCombination().getPeriod() != PeriodType.RTD) {
                    buff.append(" where ");
                    buff.append("_partitiontime >= timestamp('");
                    buff.append(totalPartitions.get(keyList.get(0)).getPartitionStartDate());
                    buff.append("') ");
                    buff.append(" and ");
                    buff.append("_partitiontime <= timestamp('");
                    buff.append(totalPartitions.get(keyList.get(keyList.size() - 1)).getPartitionEndDate());
                    buff.append("') ");
                    if (changeSetConf != null && changeSet != null) {
                        buff.append(preparaChangesetWhereClause(chngStFilter, false, true));
                    }
                    partitonStr = totalPartitions.get(keyList.get(0)).getPartitionStartDate() + " -> " + totalPartitions.get(keyList.get(keyList.size() - 1)).getPartitionEndDate();
                }
                else {
                    if (changeSetConf != null && changeSet != null) {
                        buff.append(" where ");
                        buff.append(preparaChangesetWhereClause(chngStFilter, false, false));
                    }

                }

                    String finalQuery = sqlTemplate.replace("${TBL_VAR}", fullTableVar).replace("$LOAD_DATETIME", this.loadDate).replace("$WHERE_CLAUSE", buff.toString());



                    querySet.add(buildQueryParamEntity(dimension.getShardcode(), partitonStr, SSTSpeedLayerUtil.replaceDatasetTableVariables(finalQuery, this.productDate)));


                } else {
                    if(!SSTSpeedLayerUtil.isTopPerformer(this.module)) {
                        for (Map.Entry<Long, PartitionEntity> entry : totalPartitions.entrySet()) {
                            //checks the dates are legitimate for the global week or day dimension specified for specific modules
                            if (comb.getPeriod().equals(PeriodType.WEEK) && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, LocalDate.parse(entry.getValue().getPartitionStartDate(), dateFormatWithDash).format(dateWithoutDash))) {
                                continue;
                            } else if (comb.getPeriod().equals(PeriodType.DAY) && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, LocalDate.parse(entry.getValue().getPartitionEndDate(), dateFormatWithDash).format(dateWithoutDash))) {
                                continue;
                            }
                            String partitonStr = "";
                            StringBuffer buff = new StringBuffer();
                            if (dimension.getCombination().getPeriod() != PeriodType.RTD) {
                                buff.append(" where ");
                                buff.append("_partitiontime >= timestamp('");
                                buff.append(entry.getValue().getPartitionStartDate());
                                buff.append("') ");
                                buff.append(" and ");
                                buff.append("_partitiontime <= timestamp('");
                                buff.append(entry.getValue().getPartitionEndDate());
                                buff.append("') ");
                                if (changeSetConf != null && changeSet != null) {
                                    buff.append(preparaChangesetWhereClause(chngStFilter, false, true));
                                }
                                partitonStr = entry.getValue().getPartitionStartDate() + " -> " + entry.getValue().getPartitionEndDate();
                            }
                            else {
                                if (changeSetConf != null && changeSet != null) {
                                    buff.append(" where ");
                                    buff.append(preparaChangesetWhereClause(chngStFilter, false, false));
                                }

                            }

                            String finalQuery = sqlTemplate.replace("${TBL_VAR}", fullTableVar).replace("$LOAD_DATETIME", this.loadDate).replace("$WHERE_CLAUSE", buff.toString());
                            querySet.add(buildQueryParamEntity(dimension.getShardcode(), partitonStr, SSTSpeedLayerUtil.replaceDatasetTableVariables(finalQuery, this.productDate)));

                        }
                    }else {
                        String finalQuery = sqlTemplate.replace("${TBL_VAR}", fullTableVar).replace("$LOAD_DATETIME", this.loadDate);
                        querySet.add(buildQueryParamEntity(dimension.getShardcode(), "", SSTSpeedLayerUtil.replaceDatasetTableVariables(finalQuery, this.productDate)));

                    }




                }

                //if action is changset and chnageset dataloading type is specified use them rather than dimension based loading types
                dataLoadingSchema.put(dimension.getShardcode(), chngStFilter != null && chngStFilter.getDataLoadingTypes() != null ? buildLoadingTypesSchema(chngStFilter.getDataLoadingTypes()) : buildLoadingTypesSchema(dimension.getDataLoadingType()));
                //shard


            }
            return querySet;
        }


    /**
     * genrate queries to delete/unload entires from BT table
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
        public List<QueryParamEntity> buildDeleteQuery (Integer start, Integer end) throws Exception {


            List<QueryParamEntity> deleteQueryLst = new ArrayList<QueryParamEntity>();

            Dimension[] dimensionArr = shard.getDimension();
            for (Dimension dimension : dimensionArr) {
                //check if start and end shard is provided and they current shard code is not within range, skip
                if (start != null && end != null && (dimension.getShardcode() < start || dimension.getShardcode() > end)) {
                    continue;
                }
                ChangeSetFilter chngStFilter = null;
                //if action is changset & filter exist, filter out dimension based on changeset conf
                if (this.changeSetConf != null && changeSetConf.getFilterArr() != null && changeSetConf.getFilterArr().get("remove") != null) {
                    chngStFilter = SSTSpeedLayerUtil.filterDimension(changeSetConf.getFilterArr().get("remove"), dimension);
                    if (chngStFilter == null) {
                        continue;
                    }
                }

                Combination comb = dimension.getCombination();

                String startDate = eltStartDate;
                String endDate = eltEndDate;

                //checks the dates are legitimate for the global week or day dimension specified for specific modules
                if (!SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)) {
                    if (comb.getPeriod().equals(PeriodType.WEEK) && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, this.eltStartDate)) {
                        continue;
                    } else if (comb.getPeriod().equals(PeriodType.DAY) && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, this.eltEndDate)) {
                        continue;
                    }

                    if (comb.getPeriod().equals(PeriodType.DAY)) {
                        //Modify elt start date if it is after global range for detail pages
                        startDate = SSTSpeedLayerUtil.modifyEltStartDateForDayAgg(startDate, this.module);
                    }

                    if (comb.getPeriod().equals(PeriodType.WEEK)) {
                        //Modify elt end date if it is after global range for detail modules
                        endDate = SSTSpeedLayerUtil.modifyEltEndDateForWeekAgg(endDate, this.module);
                    }
                }


                Map<Long, PartitionEntity> totalPartitions = SSTSpeedLayerUtil.partitionDateBuilder(dimension.getCombination(), startDate, endDate, this.module, false);

                List<Long> keyList = new ArrayList<Long>(totalPartitions.keySet());

                Collections.sort(keyList);

                String alternateDeleteSql = changeSetDeleteQuery.replace("${TBL_VAR}", shard.getDataset() + "." + shard.getStagingTable() + "_" + dimension.getShardcode());

                StringBuffer buff = new StringBuffer();
                buff.append(alternateDeleteSql);
                buff.append(" p ");


                if (dimension.getCombination().getPeriod() != PeriodType.RTD) {
                    buff.append(" where ");
                    buff.append("p._partitiontime >= timestamp('");
                    buff.append(totalPartitions.get(keyList.get(0)).getPartitionStartDate());
                    buff.append("') ");
                    buff.append(" and ");
                    buff.append("p._partitiontime <= timestamp('");
                    buff.append(totalPartitions.get(keyList.get(keyList.size() - 1)).getPartitionEndDate());
                    buff.append("') ");

                }


                //when moving data from staging to changeset temp table , this condition is needed
                if (dimension.getCombination().getPeriod() != PeriodType.RTD) {
                    buff.append(" AND ");
                } else if (dimension.getCombination().getPeriod() == PeriodType.RTD) {
                    buff.append(" where ");
                }


                buff.append(chngStFilter != null && chngStFilter.getSqlExpField() != null ? chngStFilter.getSqlExpField().get("Source-Staging") : this.changeSetConf.getSqlExpField().get("Source-Staging"));

                String query = buff.toString().replace("$INCLAUSE", " IN ").replace("$PARAMS", this.changeSetQuery);

                //if action is changset and chnageset dataloading type is specified use them rather than dimension based loading types
                dataLoadingSchema.put(dimension.getShardcode(), chngStFilter != null && chngStFilter.getDataLoadingTypes() != null ? buildLoadingTypesSchema(chngStFilter.getDataLoadingTypes()) : buildLoadingTypesSchema(dimension.getDataLoadingType()));

                deleteQueryLst.add(buildQueryParamEntity(dimension.getShardcode(), "", SSTSpeedLayerUtil.replaceDatasetTableVariables(query, this.productDate)));

            }

            return deleteQueryLst;

        }






}
