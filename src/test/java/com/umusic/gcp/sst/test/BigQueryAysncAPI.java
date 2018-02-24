package com.umusic.gcp.sst.test;

import com.google.cloud.bigquery.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by arumugv on 3/30/17.
 */
public class BigQueryAysncAPI {

    public static void main(final String[] args) throws IOException, InterruptedException {

        TableId shard = TableId.of("consumption_speed_layer_aggregation", "transaction_test");

        HashMap<String, Boolean> jobs = new HashMap<String, Boolean>();

        String[] partitions = {

                "2016-01-01",
                "2016-01-02",
                "2016-01-03",
                "2016-01-04",
                "2016-01-05",
                "2016-01-06",
        };

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        for (String p : partitions) {

            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(
                            "select t.transaction_date as transaction_date,\n" +
                                    "t.product_id as product_id,\n" +
                                    "t.sales_country_code as sales_country_code,\n" +
                                    "t.dma_number as dma_number,\n" +
                                    "t.country_subdivision_code as country_subdivision_code,\n" +
                                    "cast(a.master_account_code as string) as master_account_code,\n" +
                                    "sum(ifnull(t.units, cast(0 as float64))) as units,\n" +
                                    "sum(if(t.product_type = 'A' and t.subject_area = 'P', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as physical_album_units,\n" +
                                    "sum(if(t.product_type = 'A' and t.subject_area = 'D', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_album_units,\n" +
                                    "sum(if(t.product_type = 'T' and t.subject_area = 'D', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_track_units,\n" +
                                    "sum(if(t.product_type in ('T', 'V') and t.subject_area = 'S', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as stream_units,\n" +
                                    "sum(if(t.product_type = 'T' and t.subject_area = 'S', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as audio_stream_units,\n" +
                                    "sum(if(t.product_type = 'V' and t.subject_area = 'V', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as video_stream_units,\n" +
                                    "cast(0 as float64) as airplay_units,\n" +
                                    "sum(ifnull(t.euro_amount, cast(0 as float64))) as euro,\n" +
                                    "sum(if(t.product_type = 'A' and t.subject_area = 'P', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as physical_album_euro,\n" +
                                    "sum(if(t.product_type = 'A' and t.subject_area = 'D', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_album_euro,\n" +
                                    "sum(if(t.product_type = 'T' and t.subject_area = 'D', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_track_euro,\n" +
                                    "sum(if(t.product_type in ('T', 'V') and t.subject_area = 'S', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as stream_euro,\n" +
                                    "sum(if(t.product_type = 'T' and t.subject_area = 'S', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as audio_stream_euro,\n" +
                                    "sum(if(t.product_type = 'V' and t.subject_area = 'V', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as video_stream_euro,\n" +
                                    "cast(0 as float64) as airplay_euro\n" +
                                    "from (select * from `umg-marketing.consumption.transactions`\n" +
                                    "      where _partitiontime = timestamp('" + p + "') ) t\n" +
                                    "inner join `consumption_speed_layer_staging.account` a on t.sub_account_code = a.sub_account_code\n" +
                                    "group by transaction_date,\n" +
                                    "product_id,\n" +
                                    "sales_country_code,\n" +
                                    "dma_number,\n" +
                                    "country_subdivision_code,\n" +
                                    "master_account_code")

                            .setDestinationTable(shard)
                            .setUseLegacySql(false)
                            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .build();

            String uuid = UUID.randomUUID().toString();

            JobId jobId = JobId.of(uuid);

            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            jobs.put(uuid, false);
        }

        //submitted job, now lets check for it


        while (true) {

            boolean allDone = true; //not really, but if we found at least one still running we'll update

            for (String uuid : jobs.keySet()) {


                if (!jobs.get(uuid)) {

                    System.out.println("Checking job " + uuid);

                    Job queryJob = bigquery.getJob(uuid);

                    JobStatus status = queryJob.getStatus();

                    System.out.println(status.getState().toString());

                    if (status.getState() == JobStatus.State.DONE &&
                            (status.getExecutionErrors() == null ||
                                    status.getExecutionErrors().isEmpty())) {
                        jobs.replace(uuid, true);

                        System.out.println("Job " + uuid + "is done");

                    } else if (status.getState() == JobStatus.State.DONE &&
                            !status.getExecutionErrors().isEmpty()) {

                        System.out.println("Job " + uuid + "failed");
                    } else {

                        allDone = false;

                    }
                }

            }

            if (allDone) {

                System.out.println("All jobs done");

                break;

            }

            Thread.sleep(5000);


        }

    }

}
