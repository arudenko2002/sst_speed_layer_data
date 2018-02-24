package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 */
public enum Partner {
    Y(""),
    ALL("A");

    private  String marker;

    Partner(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
