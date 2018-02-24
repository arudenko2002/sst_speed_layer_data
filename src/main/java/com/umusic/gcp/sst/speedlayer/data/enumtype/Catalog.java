package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/21/17.
 * Enumtype for catalog
 */
public enum Catalog {
    Y(""),
    ALL("A");

    private  String marker;

    Catalog(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
