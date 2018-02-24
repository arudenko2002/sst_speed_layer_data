package com.umusic.gcp.sst.speedlayer.data.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by arumugv on 3/21/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Shard {

    private String project;

    private String dataset;

    private String stagingTable;

    private String crossModuleSourceStagingTable;


    private String destinationTable;

    private Dimension[] dimension;

    public Dimension[] getDimension() {
        return dimension;
    }

    public void setDimension(Dimension[] dimension) {
        this.dimension = dimension;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }


    public String getDestinationTable() {
        return destinationTable;
    }

    public void setDestinationTable(String destinationTable) {
        this.destinationTable = destinationTable;
    }

    public String getCrossModuleSourceStagingTable() {
        return crossModuleSourceStagingTable;
    }

    public void setCrossModuleSourceStagingTable(String crossModuleSourceStagingTable) {
        this.crossModuleSourceStagingTable = crossModuleSourceStagingTable;
    }

    public String getStagingTable() {
        return stagingTable;
    }

    public void setStagingTable(String stagingTable) {
        this.stagingTable = stagingTable;
    }


}
