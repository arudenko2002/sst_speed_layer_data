package com.umusic.gcp.sst.test.queryengineold.util;

import com.umusic.gcp.sst.test.queryengineold.enumtype.JoinType;

/**
 * Created by arumugv on 3/14/17.
 */
public class QueryEngineUtil {

    public static String getJoinType(JoinType joinType){
        if(joinType.equals(JoinType.INNERJOIN)){
            return JoinType.INNERJOIN.getValue();
        }
        return "";
    }
}
