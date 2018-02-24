package com.umusic.gcp.sst.speedlayer.data.entity;

/**
 * Created by arumugv on 9/6/17.
 * Entity class to create BT table from json config
 */
public class BTTableEntity {

    private String[] columnFamily;

    public String[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String[] columnFamily) {
        this.columnFamily = columnFamily;
    }




}
