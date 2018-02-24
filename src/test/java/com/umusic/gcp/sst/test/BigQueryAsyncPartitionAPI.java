package com.umusic.gcp.sst.test;

import java.io.IOException;

/**
 * Created by arumugv on 4/11/17.
 */
public class BigQueryAsyncPartitionAPI {

    public static void main(final String[] args) throws IOException, Exception {

         //BigQueryLoadAPIUtil.checkTableExists("umg-dev", "temp", "test_shard_15000101");

       // BigQueryLoadAPIUtil.createPartitionTable("umg-dev", "temp", "test_shard_15000110");

        //createPartitionTable

        /*QueryParamEntity entity = new QueryParamEntity("umg-dev", "temp", "test_shard", 15000101, false, "select transaction_date, product_id, sales_country_code from consumption_speed_layer_staging.transactions where _partitiontime = timestamp('2016-07-01') limit 30");

        entity.setPartition(20160705);

        QueryParamEntity entity1 = new QueryParamEntity("umg-dev", "temp", "test_shard", 15000102, false, "select transaction_date, product_id, sales_country_code from consumption_speed_layer_staging.transactions where _partitiontime = timestamp('2016-07-02') limit 30");

        entity1.setPartition(20160705);

        QueryParamEntity entity2 = new QueryParamEntity("umg-dev", "temp", "test_shard", 15000101, false, "select transaction_date, product_id, sales_country_code from consumption_speed_layer_staging.transactions where _partitiontime = timestamp('2016-07-01') limit 30");

        entity2.setPartition(20160706);

        QueryParamEntity entity3 = new QueryParamEntity("umg-dev", "temp", "test_shard", 15000102, false, "select transaction_date, product_id, sales_country_code from consumption_speed_layer_staging.transactions where _partitiontime = timestamp('2016-07-02') limit 30");

        entity3.setPartition(20160706);

        List<QueryParamEntity> lst = new ArrayList<QueryParamEntity>();
        lst.add(entity);
        lst.add(entity1);
        lst.add(entity2);
        lst.add(entity3);

        BigQueryLoadAPIUtil.executeAsynchronousQueries(lst);

        */

    }
}
