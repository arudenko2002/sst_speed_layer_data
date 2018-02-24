package com.umusic.gcp.sst.test.queryengineold.core;

import com.umusic.gcp.sst.test.queryengineold.entity.*;
import com.umusic.gcp.sst.test.queryengineold.util.QueryEngineUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * Created by arumugv on 3/13/17.
 */
public class SSTSelectQueryBuilder extends SSTQueryBuilder {


    public SSTSelectQueryBuilder(String json) throws IOException{
        QueryEntity entity = mapper.readValue(json, SelectQueryEntity.class);
        this.queryEntity = entity;
    }


    public String buildQuery(){
        StringBuffer queryVBuffer = new StringBuffer();

        SelectQueryEntity selectEntity = (SelectQueryEntity)this.queryEntity;

        //if(selectEntity.getType().equals(QueryType.SELECT))
            queryVBuffer.append("SELECT");

        for(int i=0; i< selectEntity.getColumn().length; i++){
            ColumnEntity column = selectEntity.getColumn()[i];
            queryVBuffer.append(" ");
            queryVBuffer.append(column.getExpression());
            if(StringUtils.isNotEmpty(column.getAs())){
                queryVBuffer.append(" ");
                queryVBuffer.append(column.getAs());
            }
            if(i < selectEntity.getColumn().length - 1){
                queryVBuffer.append(",");
            }
        }


        queryVBuffer.append(" ");
        queryVBuffer.append("FROM");
        queryVBuffer.append(" ");
        queryVBuffer.append(selectEntity.getTable().getDataset());
        queryVBuffer.append(".");
        queryVBuffer.append(selectEntity.getTable().getName());
        queryVBuffer.append(" ");
        queryVBuffer.append(selectEntity.getTable().getAlias());

        for(JoinByEntity joinBy : selectEntity.getJoinBy()){
            queryVBuffer.append(" ");
            queryVBuffer.append(QueryEngineUtil.getJoinType(joinBy.getType()));

            queryVBuffer.append(" ");
            queryVBuffer.append(joinBy.getTable().getDataset());
            queryVBuffer.append(".");
            queryVBuffer.append(joinBy.getTable().getName());

            queryVBuffer.append(" ");
            queryVBuffer.append(joinBy.getTable().getAlias());


            queryVBuffer.append(" ");
            queryVBuffer.append("on");
            queryVBuffer.append(" ");

            queryVBuffer.append(selectEntity.getTable().getAlias());
            queryVBuffer.append(".");
            queryVBuffer.append(joinBy.getOn().getPrimaryColumn());

            queryVBuffer.append(" ");
            queryVBuffer.append("=");
            queryVBuffer.append(" ");

            queryVBuffer.append(joinBy.getTable().getAlias());
            queryVBuffer.append(".");
            queryVBuffer.append(joinBy.getOn().getColumn());

        }

        if(selectEntity.getGroupBy() != null && selectEntity.getGroupBy().length > 0){
            queryVBuffer.append(" ");
            queryVBuffer.append("Group BY");
            for(int i=0; i< selectEntity.getGroupBy().length; i++){
                GroupByEntity group = selectEntity.getGroupBy()[i];

                queryVBuffer.append(" ");
                queryVBuffer.append(group.getName());

                if(i < selectEntity.getGroupBy().length - 1){
                    queryVBuffer.append(", ");
                }
            }
        }



    return queryVBuffer.toString();

    }


}
