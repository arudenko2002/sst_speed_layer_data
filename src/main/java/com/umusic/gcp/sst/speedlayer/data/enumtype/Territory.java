package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 */
public enum Territory {
    REGION("R"),
    COUNTRY("C"),
    ALL("A");

    private  String marker;

    Territory(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
