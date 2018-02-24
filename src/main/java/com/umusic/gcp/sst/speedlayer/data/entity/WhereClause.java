package com.umusic.gcp.sst.speedlayer.data.entity;

import java.util.Map;

/**
 * Created by arumugv on 4/6/17.
 * Entity class for building where clause
 */
public class WhereClause {

    private Map<String, String[]> condition;

    public Map<String, String[]> getCondition() {
        return condition;
    }

    public void setCondition(Map<String, String[]> condition) {
        this.condition = condition;
    }
}
