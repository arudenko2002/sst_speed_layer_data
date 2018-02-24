package com.umusic.gcp.sst.test.queryengineold.entity;

import com.umusic.gcp.sst.test.queryengineold.enumtype.QueryType;

/**
 * Created by arumugv on 3/13/17.
 */
public abstract class QueryEntity {

    protected QueryType type;

    protected Destination destination;

    protected TableEntity table;

    public QueryType getType() {
        return type;
    }

    public void setType(QueryType type) {
        this.type = type;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public TableEntity getTable() {
        return table;
    }

    public void setTable(TableEntity table) {
        this.table = table;
    }






}
