package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;

import java.util.Map;

/**
 * Created by arumugv on 6/4/17.
 * Entity class for changeset filer
 */
public class ChangeSetFilter {

    private Map<String, String> sqlExpField;

    private Combination combination;

    private DataLoadingType[] dataLoadingTypes;

    public Map<String, String> getSqlExpField() {
        return sqlExpField;
    }

    public void setSqlExpField(Map<String, String> sqlExpField) {
        this.sqlExpField = sqlExpField;
    }

    public Combination getCombination() {
        return combination;
    }

    public void setCombination(Combination combnation) {
        this.combination = combnation;
    }

    public DataLoadingType[] getDataLoadingTypes() {
        return dataLoadingTypes;
    }

    public void setDataLoadingTypes(DataLoadingType[] dataLoadingTypes) {
        this.dataLoadingTypes = dataLoadingTypes;
    }
}
