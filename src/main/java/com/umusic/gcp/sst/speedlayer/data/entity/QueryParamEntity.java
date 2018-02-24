package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.Module;
import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.SortedMap;

/**
 * Created by arumugv on 3/30/17.
 */
public class QueryParamEntity {

   private  String partitionLoad;

    private String project;

    private String dataset;

    private String table;

    private String destinationTable;

    private int shard;

    private boolean isDependent;

    private String query;

    private boolean isPartitionTable;

    private PeriodType periodType;

    private int partition;

    private Module module;

    private SortedMap<String, String> queryMap;


    public String getDestinationTable() {
        return destinationTable;
    }

    public void setDestinationTable(String destinationTable) {
        this.destinationTable = destinationTable;
    }

    public QueryParamEntity(int shard, String query){

        this.shard = shard;
        this.query=query;
    }


    public String getPartitionLoad() {
        return partitionLoad;
    }

    public void setPartitionLoad(String partitionLoad) {
        this.partitionLoad = partitionLoad;
    }

    public QueryParamEntity(String project, String dataset, String destinationTable, int shard, String partitionLoad, String query, Module module) {
        this.project = project;
        this.dataset = dataset;
        this.destinationTable = destinationTable;
        this.shard = shard;

        this.partitionLoad = partitionLoad;
        this.query = query;
        this.module = module;
    }

    public QueryParamEntity(String project, String dataset, String table, int shard,
                            boolean isPartitionTable, PeriodType type, SortedMap<String, String> queryMap, Module module) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.shard = shard;
        this.isPartitionTable = isPartitionTable;
        this.periodType = type;
        this.queryMap = queryMap;
        this.module = module;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public int getShard() {
        return shard;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setShard(int shard) {
        this.shard = shard;
    }

    public boolean isDependent() {
        return isDependent;
    }

    public void setDependent(boolean isDependent) {
        isDependent = isDependent;
    }


    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryParamEntity)) return false;

        QueryParamEntity that = (QueryParamEntity) o;

        return new EqualsBuilder()
                .append(project, that.getProject())
                .append(dataset, that.dataset)
                .append(table, that.table)
                .append(shard, that.shard)
                .append(isDependent, that.isDependent)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = project != null ? project.hashCode() : 0;
        result = 31 * result + (dataset != null ? dataset.hashCode() : 0);
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + shard;
        result = 31 * result + (isDependent ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);

    }


    public String toStringShard() {
        //return ToStringBuilder.reflectionToString(this);

        return String.valueOf(this.getShard()) ;
    }


    public boolean isPartitionTable() {
        return isPartitionTable;
    }

    public void setPartitionTable(boolean partitionTable) {
        isPartitionTable = partitionTable;
    }

    public PeriodType getPeriodType() {
        return periodType;
    }

    public void setPeriodType(PeriodType periodType) {
        this.periodType = periodType;
    }

    public SortedMap<String, String> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(SortedMap<String, String> queryMap) {
        this.queryMap = queryMap;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
        this.module = module;
    }
}


