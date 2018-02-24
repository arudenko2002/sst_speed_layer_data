package com.umusic.gcp.sst.test.queryengineold.entity;

import com.umusic.gcp.sst.test.queryengineold.enumtype.JoinType;

/**
 * Created by arumugv on 3/13/17.
 */
public class JoinByEntity {

    private JoinType type;

    private TableEntity table;

    private OnEntity on;

    public JoinType getType() {
        return type;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public TableEntity getTable() {
        return table;
    }

    public void setTable(TableEntity table) {
        this.table = table;
    }

    public OnEntity getOn() {
        return on;
    }

    public void setOn(OnEntity on) {
        this.on = on;
    }
}
