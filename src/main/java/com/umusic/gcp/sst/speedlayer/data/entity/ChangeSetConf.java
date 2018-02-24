package com.umusic.gcp.sst.speedlayer.data.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * Created by arumugv on 5/25/17.
 * Entity class for changeset conf
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangeSetConf {

    private Map<String, String> sqlExpField;

    private Map<String, ChangeSetFilter[]> filterArr;

    public Map<String, String> getSqlExpField() {
        return sqlExpField;
    }

    public void setSqlExpField(Map<String, String> sqlExpField) {
        this.sqlExpField = sqlExpField;
    }

    public Map<String, ChangeSetFilter[]> getFilterArr() {
        return filterArr;
    }

    public void setFilterArr(Map<String, ChangeSetFilter[]> filterArr) {
        this.filterArr = filterArr;
    }
}
