package com.umusic.gcp.sst.test;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.umusic.gcp.sst.speedlayer.data.util.BigQueryLoadAPIUtil;


import java.util.Iterator;
import java.util.List;
/**
 * Created by arumugv on 3/29/17.
 */
public class BiqQueryAPI {




    public static void sampleQueryTest(){
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder(
                                "SELECT "
                                        + "APPROX_TOP_COUNT(corpus, 10) as title, "
                                        + "COUNT(*) as unique_words "
                                        + "FROM `publicdata.samples.shakespeare`;")
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
                List<FieldValue> titles = row.get(0).getRepeatedValue();
                System.out.println("titles:");

                for (FieldValue titleValue : titles) {
                    List<FieldValue> titleRecord = titleValue.getRecordValue();
                    String title = titleRecord.get(0).getStringValue();
                    long uniqueWords = titleRecord.get(1).getLongValue();
                    System.out.printf("\t%s: %d\n", title, uniqueWords);
                }

                long uniqueWords = row.get(1).getLongValue();
                System.out.printf("total unique words: %d\n", uniqueWords);
            }

            result = result.getNextPage();
        }
    }


    public static void QueryDayTable(){
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryRequest queryRequest =
                QueryRequest
                        .newBuilder("select quarter, min(t.day) start_date, max(t.day) end_date from consumption_speed_layer_staging.day t where\n" +
                                "    quarter >= (select quarter from consumption_speed_layer_staging.day where day_id = 20170101)\n" +
                                "         and quarter <= (select quarter from consumption_speed_layer_staging.day where day_id = 20170725 ) \n" +
                                "            group by quarter order by quarter")
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
                System.out.printf("first colum " + row.get(0).getLongValue());
                System.out.printf("secon colum -start date" + row.get(1).getStringValue());
                System.out.printf("third colum - end date" + row.get(1).getStringValue());
            }

            result = result.getNextPage();
        }
    }

    public static void main(String[] args) throws Exception{
       // BiqQueryAPI.sampleQueryTest();
       // BiqQueryAPI.QueryDayTable();
        String sql = "delete from test_sst_speedlayer_agg.period_process_stack_copy where status = 'rtepostponed'";
        BigQueryLoadAPIUtil.removeProcessedDates(sql);
    }
}
