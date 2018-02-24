package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 */
public enum Genre {
    Y(""),
    ALL("A");

    private  String marker;

    Genre(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
