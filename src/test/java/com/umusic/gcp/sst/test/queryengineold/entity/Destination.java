package com.umusic.gcp.sst.test.queryengineold.entity;

/**
 * Created by arumugv on 3/13/17.
 */
public class Destination {

    private String tableName;

    private int shardNumber;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(int shardNumber) {
        this.shardNumber = shardNumber;
    }
}
