package com.umusic.gcp.sst.speedlayer.data.entity;

import java.util.Map;

/**
 * Created by arumugv on 3/13/17.
 * Entity class for mapping table config file
 */
public class TableCollectionEntity {

    private Map<String, TableEntity> tables;

    public Map<String, TableEntity> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableEntity> tables) {
        this.tables = tables;
    }
}
