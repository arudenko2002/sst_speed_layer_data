package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.JoinType;

import java.util.Map;

/**
 * Created by arumugv on 3/13/17.
 * entity class for building for table
 */
public class TableEntity {

    private Map<String, String> select;

    private WhereClause where;

    private String project;

    private String dataset;

    private String name;

    private String alias;

    private String maintablealias;

    private JoinType jointype;

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    private OnEntity on;

    public JoinType getJointype() {
        return jointype;
    }

    public void setJointype(JoinType jointype) {
        this.jointype = jointype;
    }

    public OnEntity getOn() {
        return on;
    }

    public Map<String, String> getSelect() {
        return select;
    }

    public void setSelect(Map<String, String> select) {
        this.select = select;
    }

    public void setOn(OnEntity on) {
        this.on = on;
    }

    public String getMaintablealias() {
        return maintablealias;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void setMaintablealias(String maintablealias) {
        this.maintablealias = maintablealias;
    }


    public WhereClause getWhere() {
        return where;
    }

    public void setWhere(WhereClause where) {
        this.where = where;
    }
}
