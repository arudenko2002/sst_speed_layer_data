package com.umusic.gcp.sst.test.queryengineold.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by arumugv on 3/13/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SelectQueryEntity extends QueryEntity{



    private ColumnEntity[] column;



    private JoinByEntity[] joinBy;

    private GroupByEntity[] groupBy;





    public ColumnEntity[] getColumn() {
        return column;
    }

    public void setColumn(ColumnEntity[] column) {
        this.column = column;
    }



    public GroupByEntity[] getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(GroupByEntity[] groupBy) {
        this.groupBy = groupBy;
    }



    public JoinByEntity[] getJoinBy() {
        return joinBy;
    }

    public void setJoinBy(JoinByEntity[] joinBy) {
        this.joinBy = joinBy;
    }
}
