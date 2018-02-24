package com.umusic.gcp.sst.speedlayer.data.queryengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.Schema;
import com.umusic.gcp.sst.speedlayer.data.entity.*;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.enumtype.Module;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import com.umusic.gcp.sst.speedlayer.data.util.SSTConsumptionConstants;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;


import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by arumugv on 3/13/17.
 * Queryengine  abstract class
 */
public abstract class SSTQueryEngine {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected String module;

    protected String eltStartDate;

    protected String eltEndDate;

    protected String loadDate;

    protected String productDate;

    protected String dynamicDateMode;

    protected String deltaAggMode;


    protected String changeSet;

    protected String changeSetStartDate;

    protected String changeSetEndDate;

    protected ChangeSetConf changeSetConf;


    protected String changeSetQuery;

    protected String changeSetDeleteQuery;


    protected long limitRankCount;

    protected Shard shard;


    protected Schema aggTblSchema;

    protected BTTableEntity btTableSchema;

    protected TableCollectionEntity tableCollectionEntity;

    protected String sqlTemplate;


    protected List<QueryParamEntity> querySet = new ArrayList<QueryParamEntity>();

    protected Map<Integer, Map<DataLoadingType, String>> dataLoadingSchema = new HashMap<Integer, Map<DataLoadingType, String>>();

    protected static DateTimeFormatter dateWithoutDash = DateTimeFormatter.ofPattern("yyyyMMdd");

    protected static DateTimeFormatter dateFormatWithDash = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * consutructor related to build/load
     *
     * @param module
     * @param eltStartDate
     * @param eltEndDate
     * @param loadDate
     * @param productDate
     * @param dynamicDateMode
     * @param deltaAggMode
     * @throws Exception
     */
    public SSTQueryEngine(String module, String eltStartDate, String eltEndDate, String loadDate, String productDate, String dynamicDateMode, String deltaAggMode) throws Exception {
        this.module = module;
        this.eltStartDate = eltStartDate;
        this.eltEndDate = eltEndDate;
        this.loadDate = loadDate;
        this.productDate = productDate;
        this.dynamicDateMode = dynamicDateMode;
        this.deltaAggMode = deltaAggMode;
        init();
    }

    /**
     * constructor tied with changeset
     *
     * @param module
     * @param eltStartDate
     * @param eltEndDate
     * @param loadDate
     * @param chngSet
     * @param chngStartDate
     * @param chngEndDate
     * @param conf
     * @param productDate
     * @throws Exception
     */
    public SSTQueryEngine(String module, String eltStartDate, String eltEndDate, String loadDate,
                          String chngSet,
                          String chngStartDate,
                          String chngEndDate,
                          ChangeSetConf conf,
                          String productDate
    ) throws Exception {
        this.module = module;
        this.eltStartDate = eltStartDate;
        this.eltEndDate = eltEndDate;
        this.loadDate = loadDate;
        this.changeSet = chngSet;
        this.changeSetStartDate = chngStartDate;
        this.changeSetEndDate = chngEndDate;

        this.changeSetConf = conf;
        this.productDate = productDate;
        init();
    }


    /**
     * init method load shards, sql and other needed stuffs
     *
     * @throws Exception
     */
    protected void init() throws Exception {
        loadDependentTableCollection();
        loadShard();
        loadSQLTemplate();
        loadDestinationSchema();
        if (changeSet != null) {

            loadChangesetQueryString();

            loadAlternateDeleteQuery();
        }

    }

    /**
     * get all dataloading schema
     *
     * @return
     * @throws Exception
     */
    public Map<Integer, Map<DataLoadingType, String>> getAllDataLoadingSchema() throws Exception {
        return dataLoadingSchema;
    }


    public Schema getAggregationTableSchema() {
        return aggTblSchema;
    }

    /**
     * get BT schema
     *
     * @return
     */
    public BTTableEntity getBTSchema() {
        return btTableSchema;
    }

    /**
     * Load module specific shard
     *
     * @throws IOException
     */
    protected void loadShard() throws IOException {
        Module module = Module.valueOf(this.module);
        switch (module) {
            case TOPARTIST:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_SHARD_CONFIG), Shard.class);
                break;
            case TOPPROJECT:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPPROJECT_SHARD_CONFIG), Shard.class);
                break;
            case TOPTRACK:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPTRACK_SHARD_CONFIG), Shard.class);
                break;
            case ARTIST:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ARTIST_SHARD_CONFIG), Shard.class);
                break;
            case PROJECT:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_SHARD_CONFIG), Shard.class);
                break;
            case TRACK:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TRACK_SHARD_CONFIG), Shard.class);
                break;
            case ISRC:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ISRC_SHARD_CONFIG), Shard.class);
                break;
            case ALBUM:
                shard = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_SHARD_CONFIG), Shard.class);
                break;
            default:

        }
    }


    /**
     * Genrate joins for SQL
     *
     * @param entity
     * @return
     */
    protected String prepareJoins(TableEntity entity) {
        StringBuffer queryVBuffer = new StringBuffer();
        queryVBuffer.append(" ");
        queryVBuffer.append(entity.getJointype().getDesc());

        queryVBuffer.append(" ");
        queryVBuffer.append("`");
        //commenting project to make it environment agnostic
        //queryVBuffer.append(entity.getProject());
        //queryVBuffer.append(".");
        queryVBuffer.append(entity.getDataset());
        queryVBuffer.append(".");
        queryVBuffer.append(entity.getName());
        queryVBuffer.append("`");

        queryVBuffer.append(" ");
        queryVBuffer.append(entity.getAlias());


        queryVBuffer.append(" ");
        queryVBuffer.append("on");
        queryVBuffer.append(" ");

        queryVBuffer.append(entity.getMaintablealias());
        queryVBuffer.append(".");
        queryVBuffer.append(entity.getOn().getPrimaryColumn());

        queryVBuffer.append(" ");
        queryVBuffer.append("=");
        queryVBuffer.append(" ");

        queryVBuffer.append(entity.getAlias());
        queryVBuffer.append(".");
        queryVBuffer.append(entity.getOn().getColumn());

        return queryVBuffer.toString();

    }

    /**
     * builds changeset where class
     *
     * @param chngStFilter
     * @param buildfromMetadata
     * @param andClause
     * @return
     */
    protected String preparaChangesetWhereClause(ChangeSetFilter chngStFilter, boolean buildfromMetadata, boolean andClause) {
        StringBuffer bf = new StringBuffer();
        bf.append(" ");
        if (andClause) {
            bf.append(" AND ");
        }
        if (buildfromMetadata) {
            bf.append((chngStFilter != null && chngStFilter.getSqlExpField() != null) ? chngStFilter.getSqlExpField().get("Source-Metadata") : changeSetConf.getSqlExpField().get("Source-Metadata"));
        } else {
            bf.append((chngStFilter != null && chngStFilter.getSqlExpField() != null) ? chngStFilter.getSqlExpField().get("Source-Staging") : changeSetConf.getSqlExpField().get("Source-Staging"));
        }


        return bf.toString().replace("$INCLAUSE", " IN ").replace("$PARAMS", changeSetQuery);
    }


    /**
     * load changeset query
     *
     * @throws Exception
     */
    private void loadChangesetQueryString() throws Exception {
        changeSetQuery = SSTSpeedLayerUtil.loadChangeSetSql(this.changeSet, this.changeSetStartDate, this.changeSetEndDate);


    }


    /**
     * build query param entity
     *
     * @param shardcode
     * @param partitionLoad
     * @param query
     * @return
     */
    protected QueryParamEntity buildQueryParamEntity(int shardcode, String partitionLoad, String query) {
        QueryParamEntity paramEntity = new QueryParamEntity(
                shard.getProject(),
                SSTSpeedLayerUtil.findDatasetName(shard.getDataset()),
                SSTSpeedLayerUtil.findTableName(shard.getDestinationTable()),
                shardcode,
                partitionLoad,
                query,
                Module.valueOf(this.module)

        );
        return paramEntity;

    }

    /**
     * build query param entity
     *
     * @param shardcode
     * @param isPartitionTable
     * @param type
     * @param queryMap
     * @return
     */
    protected QueryParamEntity buildQueryParamEntity(int shardcode, boolean isPartitionTable,
                                                     PeriodType type, SortedMap<String, String> queryMap) {
        QueryParamEntity paramEntity = new QueryParamEntity(
                shard.getProject(),
                SSTSpeedLayerUtil.findDatasetName(shard.getDataset()),
                SSTSpeedLayerUtil.findTableName(shard.getStagingTable()),
                shardcode,
                isPartitionTable,
                type,
                queryMap,
                Module.valueOf(this.module)
        );
        return paramEntity;

    }


    protected abstract void loadDependentTableCollection() throws IOException;

    protected abstract void loadDestinationSchema() throws IOException;

    protected abstract void loadSQLTemplate() throws IOException;


    public abstract List<QueryParamEntity> buildDeleteQuery(Integer startShard, Integer endShard) throws Exception;


    public abstract List<QueryParamEntity> buildQuery(Integer startShard, Integer endShard) throws Exception;




    protected abstract void loadAlternateDeleteQuery() throws Exception;

}
