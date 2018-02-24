package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;

/**
 * Created by arumugv on 4/18/17.
 * Entity class to transfer partition start and end date
 */
public class PartitionEntity {

    private Long period;

    private PeriodType periodType;

    private String partitionStartDate;

    private String partitionEndDate;

    private String partitionPrevStartDate;

    private String partitionPrevEndDate;

    public String getPartitionStartDate() {
        return partitionStartDate;
    }

    public void setPartitionStartDate(String partitionStartDate) {
        this.partitionStartDate = partitionStartDate;
    }

    public String getPartitionEndDate() {
        return partitionEndDate;
    }

    public void setPartitionEndDate(String partitionEndDate) {
        this.partitionEndDate = partitionEndDate;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

    public PeriodType getPeriodType() {
        return periodType;
    }

    public void setPeriodType(PeriodType periodType) {
        this.periodType = periodType;
    }

    public String getPartitionPrevStartDate() {
        return partitionPrevStartDate;
    }

    public void setPartitionPrevStartDate(String partitionPrevStartDate) {
        this.partitionPrevStartDate = partitionPrevStartDate;
    }

    public String getPartitionPrevEndDate() {
        return partitionPrevEndDate;
    }

    public void setPartitionPrevEndDate(String partitionPrevEndDate) {
        this.partitionPrevEndDate = partitionPrevEndDate;
    }
}
