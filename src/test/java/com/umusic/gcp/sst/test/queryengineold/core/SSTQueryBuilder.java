package com.umusic.gcp.sst.test.queryengineold.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.umusic.gcp.sst.test.queryengineold.entity.QueryEntity;

/**
 * Created by arumugv on 3/13/17.
 */
public abstract class SSTQueryBuilder {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected QueryEntity queryEntity;




    public abstract String buildQuery();


}
