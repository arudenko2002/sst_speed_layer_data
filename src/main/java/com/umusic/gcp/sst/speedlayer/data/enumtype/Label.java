package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 */
public enum Label {
    SAP("S"),
    ALL("A"),
    FAMILY("F");

    private  String marker;

    Label(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
