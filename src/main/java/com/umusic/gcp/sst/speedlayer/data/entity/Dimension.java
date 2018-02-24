package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;

/**
 * Created by arumugv on 3/21/17.
 */
public class Dimension {

    private int shardcode;
    
    private Combination combination;

    public CrossModuleParentShardSource getCrossModuleParentShardSource() {
        return crossModuleParentShardSource;
    }

    public void setCrossModuleParentShardSource(CrossModuleParentShardSource crossModuleParentShardSource) {
        this.crossModuleParentShardSource = crossModuleParentShardSource;
    }

    private ParentShardSource parentShardSource;

    private CrossModuleParentShardSource crossModuleParentShardSource;

    private DataLoadingType[] dataLoadingType;


    public int getShardcode() {
        return shardcode;
    }

    public DataLoadingType[] getDataLoadingType() {
        return dataLoadingType;
    }

    public void setDataLoadingType(DataLoadingType[] dataLoadingType) {
        this.dataLoadingType = dataLoadingType;
    }

    public void setShardcode(int shardcode) {
        this.shardcode = shardcode;
    }

    public Combination getCombination() {
        return combination;
    }

    public ParentShardSource getParentShardSource() {
        return parentShardSource;
    }

    public void setParentShardSource(ParentShardSource parentShardSource) {
        this.parentShardSource = parentShardSource;
    }

    public void setCombination(Combination combination) {
        this.combination = combination;
    }


}
