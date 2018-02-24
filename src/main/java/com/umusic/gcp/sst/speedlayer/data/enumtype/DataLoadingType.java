package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/29/17.
 * Enum type for loading data to BT
 */
public enum DataLoadingType {
    NODEFAULT("ND"),
    DEFAULT("D"),
    BY_PARTNER("P"),
    BY_PARTNER_SPECIFIC_PARTNER("PP"),
    BY_COUNTRY_ALL_REGION("C"),
    BY_COUNTRY_SPECIFIC_REGION("R"),
    BY_COUNTRY_SPECIFIC_COUNTRY("CC"),
    BY_COUNTRY_SUBDIVISION("CSD"),
    BY_USAGETYPE_TIER_ALL_PARTNER("UTT_PTNR"),
    BY_USAGETYPE_TIER_SPECIFIC_PARTNER("UTT");

    private  String marker;

    DataLoadingType(String marker) {
        this.marker = marker;
    }

    public String getMarker() {
        return marker;
    }
}
