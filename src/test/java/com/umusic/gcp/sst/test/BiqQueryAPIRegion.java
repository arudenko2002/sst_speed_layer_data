package com.umusic.gcp.sst.test;


import com.google.cloud.bigquery.*;

import java.util.Iterator;
import java.util.List;

/**
 * Created by arumugv on 3/29/17.
 */
public class BiqQueryAPIRegion {

    public static void main(String[] args){

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder("select distinct sales_country_code, consumption_region_code from `umg-staging.speedlayer_staging.region` order by sales_country_code" )
                        /*        "SELECT "
                                        + "APPROX_TOP_COUNT(corpus, 10) as title, "
                                        + "COUNT(*) as unique_words "
                                        + "FROM `publicdata.samples.shakespeare`;")*/
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();
        QueryResponse response = bigquery.query(queryRequest);

        QueryResult result = response.getResult();

        while (result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();

            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();

                System.out.printf("\t%s: %s\n", row.get(0).getStringValue(), row.get(1).getStringValue());


               /* List<FieldValue> titles = row.get(0).getRepeatedValue();
                System.out.println("titles:");

                for (FieldValue titleValue : titles) {
                    List<FieldValue> titleRecord = titleValue.getRecordValue();
                    String title = titleRecord.get(0).getStringValue();
                    long uniqueWords = titleRecord.get(1).getLongValue();
                    System.out.printf("\t%s: %d\n", title, uniqueWords);
                }

                long uniqueWords = row.get(1).getLongValue();
                System.out.printf("total unique words: %d\n", uniqueWords);
                */

            }

            result = result.getNextPage();
        }

    }
}
