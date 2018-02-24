package com.umusic.gcp.sst.test.queryengineold.util;

import com.umusic.gcp.sst.test.queryengineold.core.SSTQueryBuilder;
import com.umusic.gcp.sst.test.queryengineold.core.SSTSelectQueryBuilder;
import com.umusic.gcp.sst.test.queryengineold.enumtype.QueryType;

import java.io.IOException;

/**
 * Created by arumugv on 3/13/17.
 */
public class SSTQueryTypeBuilderFactory {

    public static SSTQueryBuilder getQueryTypeBuilderInstance(String type, String jsonQuery) throws IOException{
        QueryType queryType = QueryType.valueOf(type);

        switch (queryType){
            case SELECT:
                return new SSTSelectQueryBuilder(jsonQuery);
            default:
                return null;
        }

    }
}
