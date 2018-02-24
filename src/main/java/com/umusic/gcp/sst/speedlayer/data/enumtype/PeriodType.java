package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 */
public enum PeriodType {

    DAY("D"),
    WEEK("W"),
    MONTH("M"),
    QUARTER("Q"),
    YEAR("Y"),
    RTD("A");

    private  String marker;

    PeriodType(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
