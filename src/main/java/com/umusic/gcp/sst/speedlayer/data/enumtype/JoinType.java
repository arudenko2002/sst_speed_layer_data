package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/13/17.
 */
public enum JoinType {
    INNERJOIN("inner join"),
    LEFTOUTERJOIN("left outer join"),
    RIGHTOUTERJOIN("right outer join");

    private  String desc;

    JoinType(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return this.desc;
    }



}
