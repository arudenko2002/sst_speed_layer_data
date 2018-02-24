package com.umusic.gcp.sst.speedlayer.data.options;

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;

/**
 * Created by arumugv on 3/27/17.
 * Options for SST Speed layer
 */
public interface SSTSpeedLayerOptions extends CloudBigtableOptions {

    @Description("Stage value")
    @Validation.Required
    String getStage();
    void setStage(String value);

    @Description("module name ")
    @Validation.Required
    String getModule();
    void setModule(String value);

    @Description("changeset name ")
    String getChangeset();
    void setChangeset(String value);

    @Description("Start Date ")
    @Validation.Required
    String getStartDate();
    void setStartDate(String value);

    @Description("End Date ")
    @Validation.Required
    String getEndDate();
    void setEndDate(String value);

    @Description("Product Date ")
    @Validation.Required
    String getProductDate();
    void setProductDate(String value);

    @Description("ELT Load Date ")
    @Validation.Required
    String getLoadDate();
    void setLoadDate(String value);

    @Description("start shard number ")
    Integer getStartShard();
    void setStartShard(Integer value);

    @Description("end shard number ")
    Integer getEndShard();
    void setEndShard(Integer value);

    @Description("Custom Job Name ")
    String getCustomJobName();
    void setCustomJobName(String value);

    @Description("Changeset Start Date ")
    //@Validation.Required
    String getChangesetStartDate();
    void setChangesetStartDate(String value);

    @Description("changeset End Date ")
    //@Validation.Required
    String getChangesetEndDate();
    void setChangesetEndDate(String value);

    @Description("Dynamic Date Mode/INCREMENTAL MODE ")
    String getDynamicDateMode();
    void setDynamicDateMode(String value);

    @Description("Delta Aggregation  Mode, aggreagtes the delta record added that day ")
    String getDeltaAggMode();
    void setDeltaAggMode(String value);


}
