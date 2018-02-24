package com.umusic.gcp.sst.test.queryengineold.enumtype;

/**
 * Created by arumugv on 3/13/17.
 */
public enum JoinType {
    INNERJOIN("inner join"),
    LEFTOUTERJOIN("left outer join"),
    RIGHTOUTERJOIN("right outer join");

    private  String value;

    JoinType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }



}
