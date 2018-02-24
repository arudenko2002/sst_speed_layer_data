package com.umusic.gcp.sst.speedlayer.data.queryengine;

import com.umusic.gcp.sst.speedlayer.data.entity.*;
import com.umusic.gcp.sst.speedlayer.data.entity.FieldEntity;
import com.umusic.gcp.sst.speedlayer.data.enumtype.*;

import com.umusic.gcp.sst.speedlayer.data.util.SSTConsumptionConstants;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;

import com.google.cloud.bigquery.*;
import org.apache.commons.lang.StringUtils;

import java.time.LocalDate;

import java.io.IOException;
import java.util.*;

/**
 * Created by arumugv on 3/13/17.
 * Query engine class for aggregating in BQ
 */
public class SSTAggregationBuildQueryEngine extends SSTQueryEngine {


    /**
     * consutructor for build
     * @param module
     * @param eltStartDate
     * @param eltEndDate
     * @param loadDate
     * @param productDate
     * @param dynamicDateMode
     * @param deltaAggMode
     * @throws Exception
     */
    public SSTAggregationBuildQueryEngine(String module, String eltStartDate, String eltEndDate, String loadDate, String productDate, String dynamicDateMode, String deltaAggMode) throws Exception{
        super(module, eltStartDate, eltEndDate, loadDate, productDate, dynamicDateMode, deltaAggMode);
    }


    /**
     * contructor for changeset build
     * @param module
     * @param eltStartDate
     * @param eltEndDate
     * @param loadDate
     * @param changeset
     * @param chngStartDt
     * @param chngEndDate
     * @param conf
     * @param productDate
     * @throws Exception
     */
    public SSTAggregationBuildQueryEngine(String module, String eltStartDate, String eltEndDate, String loadDate,
    String changeset, String chngStartDt, String chngEndDate, ChangeSetConf conf, String productDate) throws Exception{
        super(module, eltStartDate, eltEndDate, loadDate, changeset, chngStartDt, chngEndDate,  conf, productDate);
    }

    @Override
    protected void loadShard() throws IOException {
        super.loadShard();
    }

    /**
     * load table mapping config for building joins
     * @throws IOException
     */
    protected void loadDependentTableCollection() throws IOException{
        tableCollectionEntity = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.SOURCE_TABLE_CONFIG), TableCollectionEntity.class);
    }

    /**
     * load BQ table schema
     * @throws IOException
     */
    protected void loadDestinationSchema() throws IOException{
        Module module = Module.valueOf(this.module);
        FieldEntity[] fields = null;
        switch (module){
            case TOPARTIST:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case TOPPROJECT:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case TOPTRACK:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case ARTIST:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case PROJECT:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case TRACK:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case ISRC:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            case ALBUM:
                fields = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_AGGREGATION_TABLE_CONFIG), FieldEntity[].class);
                break;
            default:
                ;
        }
        List<Field> desFields = new ArrayList<>();
        for(FieldEntity bean : fields){
            desFields.add(Field.of(bean.getName(), SSTSpeedLayerUtil.getTypes(bean.getType())));
        }
        aggTblSchema = Schema.of(desFields);

    }

    /**
     *  Load alternate delete query for changeset
     *  sql to add negative consumption units for affected entities
     * @throws Exception
     */
    protected  void loadAlternateDeleteQuery() throws Exception{
        Module mod = Module.valueOf(module);
        switch (mod){
            case TRACK:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_ALTERNATE_DELETE_SQL);
                break;
            case PROJECT:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_ALTERNATE_DELETE_SQL);
                break;
            case ARTIST:
                changeSetDeleteQuery = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_ALTERNATE_DELETE_SQL);
                break;
            default:
                return ;
        }
    }


    /**
     * load sql template for building aggregation
     * @throws IOException
     */
    protected void loadSQLTemplate() throws IOException{
        Module module = Module.valueOf(this.module);
        switch (module){
            case TOPARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_STAGE1_SQLTEMPLATE);
                break;
            case TOPPROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_STAGE1_SQLTEMPLATE);
                break;
            case TOPTRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_STAGE1_SQLTEMPLATE);
                break;
            case ARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_STAGE1_SQLTEMPLATE);
                break;
            case PROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_STAGE1_SQLTEMPLATE);
                break;
            case TRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_STAGE1_SQLTEMPLATE);
                break;
            case ISRC:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_AGG_SQLTEMPLATE);
                break;
            case ALBUM:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_AGG_SQLTEMPLATE);
                break;
            default:
                ;
        }
    }

    /**
     * overload sqlTemplate with sql from building from parent shard
     * @throws IOException
     */
    private void overloadWithShardSQlTemplate() throws IOException{
        Module module = Module.valueOf(this.module);
        switch (module){
            case TOPARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_PARENT_SHARD_SQLTEMPLATE);
                break;
            case TOPPROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_PARENT_SHARD_SQLTEMPLATE);
                break;
            case TOPTRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_PARENT_SHARD_SQLTEMPLATE);
                break;
            case ARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_PARENT_SHARD_SQLTEMPLATE);
                break;
            case PROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_PARENT_SHARD_SQLTEMPLATE);
                break;
            case TRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_PARENT_SHARD_SQLTEMPLATE);
                break;
            case ISRC:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_AGG_PARENTSHARD_SQLTEMPLATE);
                break;
            case ALBUM:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_AGG_PARENTSHARD_SQLTEMPLATE);
                break;
            default:
                ;
        }
    }

    /**
     * overload sqlTemplate with cross module sql
     * @throws IOException
     */
    private void overloadWithCrossModuleShardSQlTemplate() throws IOException{
        Module module = Module.valueOf(this.module);
        switch (module){
            case TOPARTIST:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE);
                break;
            case TOPPROJECT:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE);
                break;
            case TOPTRACK:
                sqlTemplate = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE);
                break;
            default:
                ;
        }
    }

    /**
     * build select params
     * @param comb
     */
    protected void buildAllSelectParams(Combination comb){

        TableEntity periodTable = tableCollectionEntity.getTables().get("period");
        TableEntity terriTable = tableCollectionEntity.getTables().get("territory");
        TableEntity terriSubDivTable = tableCollectionEntity.getTables().get("territorysubdivision");
        TableEntity partnerTable = tableCollectionEntity.getTables().get("partner");
        TableEntity labelTable = tableCollectionEntity.getTables().get("label");
        TableEntity  usageTypeTable = tableCollectionEntity.getTables().get("usagetype");
        TableEntity  projectGenreTable = tableCollectionEntity.getTables().get("projectGenre");

        TableEntity  trackGenreTable = tableCollectionEntity.getTables().get("trackGenre");

        TableEntity  projCatalogTable = tableCollectionEntity.getTables().get("projectcatalog");

        TableEntity  trackCatalogTable = tableCollectionEntity.getTables().get("trackcatalog");

        String periodField = periodTable.getSelect().get(comb.getPeriod().name());
        String periodMarker = comb.getPeriod().getMarker();
        sqlTemplate = sqlTemplate.replace("${PERIOD_MARKER}", "'" + periodMarker + "'")
                .replace("${PERIOD}", periodField)
                .replace("${TERRITORY_MARKER}", "'" + comb.getTerritory().getMarker() + "'")
                .replace("${TERRITORY}" , terriTable.getSelect().get(comb.getTerritory().name()))
                .replace("${PARTNER}" , partnerTable.getSelect().get(comb.getPartner().name()))
                .replace("${LABEL_MARKER}", "'" + comb.getLabel().getMarker() + "'")
                .replace("${LABEL}" , labelTable.getSelect().get(comb.getLabel().name())

                );
        //territory sub division Mandatory for few modules
        if(comb.getTerritorysubdivision() != null){
            sqlTemplate = sqlTemplate.replace("${TERRITORY_SUBDIVISION}" , terriSubDivTable.getSelect().get(comb.getTerritorysubdivision().name()));
        }

        //proj catalog Mandatory for few modules
        if(comb.getProjectCatalog() != null){
            sqlTemplate = sqlTemplate.replace("${CATALOG}" , projCatalogTable.getSelect().get(comb.getProjectCatalog().name()));
        }

        //track catalog Mandatory for few modules
        if(comb.getTrackCatalog() != null){
            sqlTemplate = sqlTemplate.replace("${CATALOG}" , trackCatalogTable.getSelect().get(comb.getTrackCatalog().name()));
        }

        //proj genre Mandatory for few modules
        if(comb.getProjectGenre() != null){
            sqlTemplate = sqlTemplate.replace("${PROJECT_GENRE}" , projectGenreTable.getSelect().get(comb.getProjectGenre().name()));
        }

        //track genre Mandatory for few modules
        if(comb.getTrackGenre() != null){
            sqlTemplate = sqlTemplate.replace("${TRACK_GENRE}" , trackGenreTable.getSelect().get(comb.getTrackGenre().name()));
        }

        //usage type Mandatory for few modules
        if(comb.getUsagetype() != null){
            sqlTemplate = sqlTemplate.replace("${USAGE_TYPE_TIER}" , usageTypeTable.getSelect().get(comb.getUsagetype().name()));
        }
    }

    /**
     * build cross module specific select params
     * @param comb
     * @param crossModuleParentShardSource
     */
    protected void setCrossModulePeriodSelectParams(Combination comb, CrossModuleParentShardSource crossModuleParentShardSource) {
        TableEntity periodTable = tableCollectionEntity.getTables().get("period");


        String periodField = periodTable.getSelect().get(comb.getPeriod().name());
        String periodMarker = comb.getPeriod().getMarker();

        sqlTemplate = sqlTemplate.replace("${PERIOD_MARKER}", "'" + periodMarker + "'")
                .replace("${PERIOD}", periodField);


        sqlTemplate = sqlTemplate.replace("${CP_PARENT_PERIOD_COLUMN}", periodTable.getSelect().get(crossModuleParentShardSource.getTpPeriod().name()))
        .replace("${LPTD_PARENT_PERIOD_COLUMN}", periodTable.getSelect().get(crossModuleParentShardSource.getLptdPeriod().name()));
    }


    /**
     * set period select params
     * @param comb
     * @param parent
     */
    protected void setPeriodSelectParams(Combination comb, ParentShardSource parent) {
        TableEntity periodTable = tableCollectionEntity.getTables().get("period");


        String periodField = periodTable.getSelect().get(comb.getPeriod().name());
        String periodMarker = comb.getPeriod().getMarker();

        sqlTemplate = sqlTemplate.replace("${PERIOD_MARKER}", "'" + periodMarker + "'")
                .replace("${PERIOD}", periodField);


        sqlTemplate = sqlTemplate.replace("${PARENT_PERIOD_COLUMN}", periodTable.getSelect().get(parent.getPeriod().name()));
    }

    /**
     * set the cross module parent shard table
     * @param comb
     * @param crossModuleParentShardSource
     */
    protected void setCrossModuleParentShardTable(Combination comb, CrossModuleParentShardSource crossModuleParentShardSource){

        StringBuffer queryCpBuffer = new StringBuffer();
        queryCpBuffer.append("`")
                .append(shard.getDataset())
                .append(".")
                //.append((changeSetConf != null && changeSet != null) ? shard.getChangesetTempTable() : shard.getStagingTable())
                .append(shard.getCrossModuleSourceStagingTable())
                .append("_")
                .append(crossModuleParentShardSource.getTpShardCode())
                .append("`");


        StringBuffer queryLPTDBuffer = new StringBuffer();
        queryLPTDBuffer.append("`")
                .append(shard.getDataset())
                .append(".")
                //.append((changeSetConf != null && changeSet != null) ? shard.getChangesetTempTable() : shard.getStagingTable())
                .append(shard.getCrossModuleSourceStagingTable())
                .append("_")
                .append(crossModuleParentShardSource.getLptdShardCode())
                .append("`");


        sqlTemplate = sqlTemplate.replace("${CP_PARENT_SHARD_TABLE}", queryCpBuffer.toString())
                .replace("${LPTD_PARENT_SHARD_TABLE}", queryLPTDBuffer.toString());




    }


    /**
     * set parent shard table
     * @param comb
     * @param parent
     */
    protected void setParentShardTable(Combination comb, ParentShardSource parent){

        StringBuffer queryTblBuffer = new StringBuffer();
        queryTblBuffer.append("`")
                .append(shard.getDataset())
                .append(".")
                //.append((changeSetConf != null && changeSet != null) ? shard.getChangesetTempTable() : shard.getStagingTable())
                .append(shard.getStagingTable())
                .append("_")
                .append(parent.getShardcode())
                .append("`");
        sqlTemplate = sqlTemplate.replace("${PARENT_SHARD_TABLE}", queryTblBuffer.toString());




    }

    /**
     * build all addiitonal join
     * @param comb
     */
    protected void buildAllAdditionalJoins(Combination comb){
        StringBuffer queryVBuffer = new StringBuffer();
        if(!comb.getPeriod().equals(PeriodType.RTD)){
            queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("period")));
        }
        if(!comb.getTerritory().equals(Territory.ALL) && !comb.getTerritory().equals(Territory.COUNTRY)){
            queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("territory")));
        }
        //partner join is not needed anymore since master_account_available in transaction
       /* if(!comb.getPartner().equals(Partner.ALL)){
            queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("partner")));
        }*/

        //label join is not needed anymore since sap_segment_code is  in transaction
       /* if(!comb.getLabel().equals(Label.ALL)){
            queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("label")));
        }*/
        /* removing the genre from the aggregation. Might be added back in the future. Commenting this line won't really change the results
        * since it checks for Genre.ALL but just in case something is changed
       if(comb.getProjectGenre() != null && !comb.getProjectGenre().equals(Genre.ALL)){
           queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("projectGenre")));
       }

        if(comb.getTrackGenre() != null && !comb.getTrackGenre().equals(Genre.ALL)){
            queryVBuffer.append(prepareJoins(tableCollectionEntity.getTables().get("trackGenre")));
        }
        */

        sqlTemplate = sqlTemplate.replace("${ADDITIONAL_JOIN}", queryVBuffer.toString());


    }

    /**
     * build partition where clause
     * @param partitionEntity
     */
    private void buildPartitionWhereCaluse(PartitionEntity partitionEntity){

        sqlTemplate = sqlTemplate.replace("${PARTITION_START_DATE}" , partitionEntity.getPartitionStartDate())
                        .replace("${PARTITION_END_DATE}" , partitionEntity.getPartitionEndDate());

        if(StringUtils.isNotEmpty(partitionEntity.getPartitionPrevStartDate()) && StringUtils.isNotEmpty(partitionEntity.getPartitionPrevEndDate())){
            sqlTemplate = sqlTemplate.replace("${PARTITION_PREV_START_DATE}" , partitionEntity.getPartitionPrevStartDate())
                    .replace("${PARTITION_PREV_END_DATE}" , partitionEntity.getPartitionPrevEndDate());
        }

    }

    /**
     * build delta Agg where clause
     * @param buildFromMetadata
     */
    private void buildDeltaAggModeWhereClause(boolean buildFromMetadata){
        if(SSTSpeedLayerUtil.isDeltaAggregationMode(this.deltaAggMode)){
            if(buildFromMetadata){
                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_WHERE_CALUSE}" , " AND cast($ALIAS_TRANSACTION.load_datetime as date) between cast('${INCREMENTAL_LOAD_START_DATE}' as date) and cast('${INCREMENTAL_LOAD_END_DATE}' as date) ");
                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_LOAD_START_DATE}", LocalDate.parse(eltStartDate, dateWithoutDash).format(dateFormatWithDash));

                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_LOAD_END_DATE}", LocalDate.parse(eltEndDate, dateWithoutDash).format(dateFormatWithDash));

            }else{
                //in case of building higher level shard table from lower level filer by load date ,
                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_WHERE_CALUSE}" , " AND cast($ALIAS_PARENT_SHARD.load_datetime as date) between  cast('${INCREMENTAL_LOAD_START_DATE}' as date) and cast('${INCREMENTAL_LOAD_END_DATE}' as date) ");
                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_LOAD_START_DATE}", this.loadDate);

                sqlTemplate = sqlTemplate.replace("${INCREMENTAL_LOAD_END_DATE}", this.loadDate);

            }


        }else{
            sqlTemplate = sqlTemplate.replace("${INCREMENTAL_WHERE_CALUSE}" , " ");
        }

    }

    /**
     * build changeset clause
     * @param chngStFilter
     * @param buildFromMetadata
     */
    private void buildChangeSetClause(ChangeSetFilter chngStFilter, boolean buildFromMetadata){
        StringBuffer queryVBuffer = new StringBuffer();
        if(changeSetConf != null && changeSet != null){
            queryVBuffer.append(preparaChangesetWhereClause(chngStFilter, buildFromMetadata, true));
        }
        sqlTemplate = sqlTemplate.replace("${CHANGESET_CLAUSE}" , StringUtils.isEmpty(queryVBuffer.toString()) ? "" : queryVBuffer.toString());

    }

    private void replaceLoadTime(){
        sqlTemplate = sqlTemplate.replace("${LOAD_DATETIME}" , this.loadDate);
    }

    private void setShardNumber(int shardNumber){
        sqlTemplate = sqlTemplate.replace("${SHARD_NUMBER}" , String.valueOf(shardNumber));
    }


    /**
     * build all adiditonal where clause
     * @param comb
     * @param whereClauseExist
     */
    private void buildAllAdditionalWhereCaluse(Combination comb, boolean whereClauseExist){
        StringBuffer queryVBuffer = new StringBuffer();

       if(!comb.getLabel().equals(Label.ALL) ){
           TableEntity labelTable = tableCollectionEntity.getTables().get("label");
           if(labelTable.getWhere() != null && labelTable.getWhere().getCondition().containsKey(comb.getLabel().name()) &&
                   labelTable.getWhere().getCondition().get(comb.getLabel().name()).length > 0){
               //queryVBuffer.append("where");
              //todo fix the whereclause exists, maybe not needed
               String arr[] = labelTable.getWhere().getCondition().get(comb.getLabel().name());
               for(int i=0; i < arr.length; i++){
                  if(whereClauseExist){
                      queryVBuffer.append(" ");
                      queryVBuffer.append(" AND ");
                      queryVBuffer.append(arr[i]);
                  }else{
                      if( i ==0 ) {
                          queryVBuffer.append(" where");
                          queryVBuffer.append(" ");
                          queryVBuffer.append(arr[i]);
                      }
                      else{
                          queryVBuffer.append(" ");
                          queryVBuffer.append(" AND ");
                          queryVBuffer.append(arr[i]);
                      }
                  }

               }
           }

        }

        if(comb.getTerritory() != null  ){
            //&& comb.getTerritory().equals(Territory.REGION)
            TableEntity terriTable = tableCollectionEntity.getTables().get("territory");
            if(terriTable.getWhere() != null && terriTable.getWhere().getCondition().containsKey(comb.getTerritory().name()) ){
                //queryVBuffer.append("where");

                for(int i=0; i < terriTable.getWhere().getCondition().get(comb.getTerritory().name()).length; i++){
                    if(whereClauseExist){
                        queryVBuffer.append(" ");
                        queryVBuffer.append(" AND ");
                        queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritory().name())[i]);
                    }else{
                        if( i ==0 ) {
                            queryVBuffer.append(" where");
                            queryVBuffer.append(" ");
                            queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritory().name())[i]);
                        }
                        else{
                            queryVBuffer.append(" ");
                            queryVBuffer.append(" AND ");
                            queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritory().name())[i]);
                        }
                    }


                }
            }

        }

        if(comb.getTerritorysubdivision() != null && !comb.getTerritorysubdivision().equals(TerritorySubDivision.ALL) ){
            TableEntity terriTable = tableCollectionEntity.getTables().get("territorysubdivision");
            if(terriTable.getWhere() != null && terriTable.getWhere().getCondition().containsKey(comb.getTerritorysubdivision().name())){


                for(int i=0; i < terriTable.getWhere().getCondition().get(comb.getTerritorysubdivision().name()).length; i++){
                    if(whereClauseExist){
                        queryVBuffer.append(" ");
                        queryVBuffer.append(" AND ");
                        queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritorysubdivision().name())[i]);
                    }else{
                        if( i ==0 ) {
                            queryVBuffer.append(" where");
                            queryVBuffer.append(" ");
                            queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritorysubdivision().name())[i]);
                        }
                        else{
                            queryVBuffer.append(" ");
                            queryVBuffer.append(" AND ");
                            queryVBuffer.append(terriTable.getWhere().getCondition().get(comb.getTerritorysubdivision().name())[i]);
                        }
                    }


                }
            }

        }


        if(comb.getProjectCatalog() != null  ){
            TableEntity prodCatTable = tableCollectionEntity.getTables().get("projectcatalog");
            if(prodCatTable.getWhere() != null && prodCatTable.getWhere().getCondition().containsKey(comb.getProjectCatalog().name()) ){
                for(int i=0; i < prodCatTable.getWhere().getCondition().get(comb.getProjectCatalog().name()).length; i++){
                    if(whereClauseExist){
                        queryVBuffer.append(" ");
                        queryVBuffer.append(" AND ");
                        queryVBuffer.append(prodCatTable.getWhere().getCondition().get(comb.getProjectCatalog().name())[i]);
                    }else{
                        if( i ==0 ) {
                            queryVBuffer.append(" where");
                            queryVBuffer.append(" ");
                            queryVBuffer.append(prodCatTable.getWhere().getCondition().get(comb.getProjectCatalog().name())[i]);
                        }
                        else{
                            queryVBuffer.append(" ");
                            queryVBuffer.append(" AND ");
                            queryVBuffer.append(prodCatTable.getWhere().getCondition().get(comb.getProjectCatalog().name())[i]);
                        }
                    }


                }
            }

        }

        if(comb.getTrackCatalog() != null  ){
            //&& comb.getTerritory().equals(Territory.REGION)
            TableEntity trackCatTable = tableCollectionEntity.getTables().get("trackcatalog");
            if(trackCatTable.getWhere() != null && trackCatTable.getWhere().getCondition().containsKey(comb.getTrackCatalog().name()) ){
                //queryVBuffer.append("where");

                for(int i=0; i < trackCatTable.getWhere().getCondition().get(comb.getTrackCatalog().name()).length; i++){
                    if(whereClauseExist){
                        queryVBuffer.append(" ");
                        queryVBuffer.append(" AND ");
                        queryVBuffer.append(trackCatTable.getWhere().getCondition().get(comb.getTrackCatalog().name())[i]);
                    }else{
                        if( i ==0 ) {
                            queryVBuffer.append(" where");
                            queryVBuffer.append(" ");
                            queryVBuffer.append(trackCatTable.getWhere().getCondition().get(comb.getTrackCatalog().name())[i]);
                        }
                        else{
                            queryVBuffer.append(" ");
                            queryVBuffer.append(" AND ");
                            queryVBuffer.append(trackCatTable.getWhere().getCondition().get(comb.getTrackCatalog().name())[i]);
                        }
                    }


                }
            }

        }

        TableEntity productTable = tableCollectionEntity.getTables().get("product");
        if(productTable.getWhere() != null && productTable.getWhere().getCondition().containsKey("mandatory")){
            //queryVBuffer.append("where");

            for(int i=0; i < productTable.getWhere().getCondition().get("mandatory").length; i++){
                  /* if( i ==0 ){
                       queryVBuffer.append(" ");
                       queryVBuffer.append(labelTable.getWhere().getCondition()[i]);
                   }else{ */
                queryVBuffer.append(" ");
                queryVBuffer.append(" AND ");
                queryVBuffer.append(productTable.getWhere().getCondition().get("mandatory")[i]);
                // }
            }
        }


        sqlTemplate = sqlTemplate.replace("${ADDITIONAL_WHERE_CALUSE}", queryVBuffer.toString());


    }


    /**
     * build query per partition
     * @param comb
     * @param shardCode
     * @param parentShard
     * @param crossModuleParentShardSource
     * @param chngStFilter
     * @throws Exception
     */
    private void buildQueryPerPartition(Combination comb, int shardCode, ParentShardSource parentShard, CrossModuleParentShardSource crossModuleParentShardSource, ChangeSetFilter chngStFilter) throws Exception{
        Map<Long, PartitionEntity> totalPartitions = null;
        String startDate = eltStartDate;
        String endDate = eltEndDate;

        //may have to alter start date end date in static elt start/end date if the dates are out of range for weekly or daily shard for detail modules
        if(!SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)){

            if(comb.getPeriod().equals(PeriodType.DAY)){
                //Modify elt start date if it is after global range for detail pages
                startDate = SSTSpeedLayerUtil.modifyEltStartDateForDayAgg(startDate, this.module);
            }

            if(comb.getPeriod().equals(PeriodType.WEEK)){
                //Modify elt end date if it is after global range for detail modules
                endDate = SSTSpeedLayerUtil.modifyEltEndDateForWeekAgg(endDate, this.module);
            }
        }


        totalPartitions = SSTSpeedLayerUtil.partitionDateBuilder(comb, startDate, endDate, this.module, SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode));

        SortedMap<String, String> queryMap = new TreeMap<String, String>();
        for(Map.Entry<Long, PartitionEntity> entry : totalPartitions.entrySet()){

            //Applicable for dynamic mode,
            if(SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)){
                //checks the dates are legitimate for the global week or day dimension specified for specific modules
                if(comb.getPeriod().equals(PeriodType.WEEK)  && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, LocalDate.parse(entry.getValue().getPartitionStartDate(), dateFormatWithDash).format(dateWithoutDash))){
                    continue;
                }else if(comb.getPeriod().equals(PeriodType.DAY)  && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, LocalDate.parse(entry.getValue().getPartitionEndDate(), dateFormatWithDash).format(dateWithoutDash))){
                    continue;
                }
            }

            //build from parent shard table
            if(crossModuleParentShardSource != null && SSTSpeedLayerUtil.isCrossModuleBuild()){
                overloadWithCrossModuleShardSQlTemplate();
                setCrossModulePeriodSelectParams(comb, crossModuleParentShardSource);
                setCrossModuleParentShardTable(comb, crossModuleParentShardSource);
                buildPartitionWhereCaluse(entry.getValue());
                replaceLoadTime();

            }
            else if(parentShard != null && SSTSpeedLayerUtil.validateMonthCanbeBuildFromDays(entry.getValue().getPartitionStartDate(), this.module, comb, parentShard)
                   // && changeSetConf == null && changeSet == null
                    ){
                overloadWithShardSQlTemplate();
                setPeriodSelectParams(comb, parentShard);
                setParentShardTable(comb, parentShard);
                buildPartitionWhereCaluse(entry.getValue());
                buildDeltaAggModeWhereClause(false);
                buildChangeSetClause(chngStFilter, false);

            }else{ // build from transactions and metadata table
                loadSQLTemplate();
                buildAllSelectParams(comb);
                buildAllAdditionalJoins(comb);
                buildPartitionWhereCaluse(entry.getValue());
                buildDeltaAggModeWhereClause(true);
                replaceLoadTime();
                buildChangeSetClause(chngStFilter, true);
                //setShardNumber(shardCode);
                buildAllAdditionalWhereCaluse(comb, true);

            }
            setShardNumber(shardCode);
            LocalDate startPart = LocalDate.parse(entry.getValue().getPartitionStartDate(), dateFormatWithDash);
            queryMap.put(startPart.format(dateWithoutDash), SSTSpeedLayerUtil.replaceDatasetTableVariables(sqlTemplate, this.productDate));





        }
        QueryParamEntity paramEntity = null;


        paramEntity =  buildQueryParamEntity(shardCode, comb.getPeriod() != PeriodType.RTD ? true : false, comb.getPeriod(), queryMap);

        querySet.add(paramEntity);

    }


    /**
     * generate query from start to end
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public List<QueryParamEntity>  buildQuery(Integer start, Integer end) throws Exception{

        Dimension[] dimensionArr = shard.getDimension();
        for (Dimension dimension : dimensionArr) {
            //check if start and end shard is provided and they current shard code is not within range, skip
            if(start != null && end != null && (dimension.getShardcode() < start || dimension.getShardcode() > end)){
                continue;
            }
            ChangeSetFilter chngStFilter = null;
            //if action is changset & filter exist, filter out dimension based on changeset conf
            if(this.changeSetConf != null && changeSetConf.getFilterArr() != null && changeSetConf.getFilterArr().get("build") != null){
                chngStFilter = SSTSpeedLayerUtil.filterDimension(changeSetConf.getFilterArr().get("build"), dimension);
                if(chngStFilter == null){
                    continue;
                }
            }
            Combination comb = dimension.getCombination();

            //Applicable for non dynamic mode
            if(!SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)){
                //checks the dates are legitimate for the global week or day dimension specified for specific modules
                if(comb.getPeriod().equals(PeriodType.WEEK)  && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, this.eltStartDate)){
                    continue;
                }else if(comb.getPeriod().equals(PeriodType.DAY)  && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, this.eltEndDate)){
                    continue;
                }
            }


            buildQueryPerPartition(comb, dimension.getShardcode(), dimension.getParentShardSource(), dimension.getCrossModuleParentShardSource(), chngStFilter);




        }
        return querySet;
    }




    /**
     * build alternate delete query for changeset
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    @Override
    public List<QueryParamEntity> buildDeleteQuery(Integer start, Integer end) throws Exception{


        List<QueryParamEntity> queryLst = new ArrayList<QueryParamEntity>();

        Dimension[] dimensionArr = shard.getDimension();
        for (Dimension dimension : dimensionArr) {
            //check if start and end shard is provided and they current shard code is not within range, skip
            if(start != null && end != null && (dimension.getShardcode() < start || dimension.getShardcode() > end)){
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

            //may have to alter start date end date in static elt start/end date if the dates are out of range for weekly or daily shard for detail modules
            if(!SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)){

                //checks the dates are legitimate for the global week or day dimension specified for specific modules
                if(comb.getPeriod().equals(PeriodType.WEEK)  && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, this.eltStartDate)){
                    continue;
                }else if(comb.getPeriod().equals(PeriodType.DAY)  && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, this.eltEndDate)){
                    continue;
                }

                if(comb.getPeriod().equals(PeriodType.DAY)){
                    //Modify elt start date if it is after global range for detail pages
                    startDate = SSTSpeedLayerUtil.modifyEltStartDateForDayAgg(startDate, this.module);
                }

                if(comb.getPeriod().equals(PeriodType.WEEK)){
                    //Modify elt end date if it is after global range for detail modules
                    endDate = SSTSpeedLayerUtil.modifyEltEndDateForWeekAgg(endDate, this.module);
                }
            }

            // dynamic dates is false for changeset, we are processing only range dates.
            Map<Long, PartitionEntity> totalPartitions = SSTSpeedLayerUtil.partitionDateBuilder(dimension.getCombination(), startDate, endDate, this.module, false);
            SortedMap<String, String> queryMap = new TreeMap<String, String>();
            for(Map.Entry<Long, PartitionEntity> entry : totalPartitions.entrySet()){

                //Applicable for dynamic mode,
                if(SSTSpeedLayerUtil.isDynamicMode(this.dynamicDateMode)){
                    //checks the dates are legitimate for the global week or day dimension specified for specific modules
                    if(comb.getPeriod().equals(PeriodType.WEEK)  && !SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange(this.module, LocalDate.parse(entry.getValue().getPartitionStartDate(), dateFormatWithDash).format(dateWithoutDash))){
                        continue;
                    }else if(comb.getPeriod().equals(PeriodType.DAY)  && !SSTSpeedLayerUtil.isDatesWithinGlobalDayRange(this.module, LocalDate.parse(entry.getValue().getPartitionEndDate(), dateFormatWithDash).format(dateWithoutDash))){
                        continue;
                    }
                }

                String alternateDeleteSql = changeSetDeleteQuery.replace("${TBL_VAR}" , shard.getDataset() + "." + shard.getStagingTable() + "_" + dimension.getShardcode()).replace("${LOAD_DATETIME}", this.loadDate);

                StringBuffer buff = new StringBuffer();
                buff.append(alternateDeleteSql);
                buff.append(" p ");

                if(dimension.getCombination().getPeriod() != PeriodType.RTD){
                    buff.append(" where ");
                    buff.append("p._partitiontime >= timestamp('");
                    buff.append(entry.getValue().getPartitionStartDate());
                    buff.append("') ");
                    buff.append(" and ");
                    buff.append("p._partitiontime <= timestamp('");
                    buff.append(entry.getValue().getPartitionEndDate());
                    buff.append("') ");

                }

                //String query = "";

                //when moving data from staging to changeset temp table , this condition is needed
                if(dimension.getCombination().getPeriod() != PeriodType.RTD ){
                    buff.append(" AND ");
                }else if (dimension.getCombination().getPeriod() == PeriodType.RTD){
                    buff.append(" where ");
                }
                buff.append(chngStFilter != null && chngStFilter.getSqlExpField() != null ? chngStFilter.getSqlExpField().get("Source-Staging") : this.changeSetConf.getSqlExpField().get("Source-Staging"));
                // query = buff.toString().replace("$INCLAUSE", isStageToChangesetTable == true ? " NOT IN " : " IN ").replace("$PARAMS", this.changeSetQuery);
                alternateDeleteSql = buff.toString().replace("$INCLAUSE",  " IN " ).replace("$PARAMS", this.changeSetQuery);

                LocalDate startPart = LocalDate.parse(entry.getValue().getPartitionStartDate(), dateFormatWithDash);

                queryMap.put(startPart.format(dateWithoutDash), SSTSpeedLayerUtil.replaceDatasetTableVariables(alternateDeleteSql, this.productDate));
            }
            //build QueryParamEntity
            QueryParamEntity paramEntity = null;
            // override destination table based on param {isStageToChangesetTable}

            paramEntity = buildQueryParamEntity(dimension.getShardcode(), dimension.getCombination().getPeriod() != PeriodType.RTD ? true : false, dimension.getCombination().getPeriod(), queryMap);

            queryLst.add(paramEntity);

        }

        return queryLst;

    }



}
