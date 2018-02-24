package com.umusic.gcp.sst.test;

//import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableConfiguration;
import com.umusic.gcp.sst.speedlayer.data.entity.BTTableEntity;
import com.umusic.gcp.sst.speedlayer.data.util.BigtableAdminAPIUtil;
import com.umusic.gcp.sst.speedlayer.data.util.SSTConsumptionConstants;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by arumugv on 9/6/17.
 */
public class BigtableJavaAPITest {

    public static void main(String[] args) throws IOException{
        String projectId= "umg-swift";
        String instanceId = "swift-consumption";

        BigtableAdminAPIUtil obj = new BigtableAdminAPIUtil(projectId, instanceId);
        BTTableEntity schena = new ObjectMapper().readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.ALBUM_BT_TABLE_SCHEMA), BTTableEntity.class);

        obj.createTable("test_table1", schena);
       obj.closeConnection();
       // Connection connection = BigtableConfiguration.connect(projectId, instanceId);
       // Admin admin = connection.getAdmin();
      //  System.out.println(admin.tableExists(TableName.valueOf("top_track_uat2_v1")));

        //Table table = connection.getTable(TableName.valueOf("top_tracking_uat_v1"));
       // table.
       // System.out.println("table is " + table);
        //table.getTableDescriptor().toString();

    }
       // try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {


        // The admin API lets us create, manage and delete tables

            // [END connecting_to_bigtable]

            // [START creating_a_table]
            // Create a table with a single column family
           /* HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

            print("Create table " + descriptor.getNameAsString());
            admin.createTable(descriptor);*/
            // [END creating_a_table]

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
           // Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            // Write some rows to the table
          /*  print("Write some greetings to the table");
            for (int i = 0; i < GREETINGS.length; i++) {
                // Each row has a unique row key.
                //
                // Note: This example uses sequential numeric IDs for simplicity, but
                // this can result in poor performance in a production application.
                // Since rows are stored in sorted order by key, sequential keys can
                // result in poor distribution of operations across nodes.
                //
                // For more information about how to design a Bigtable schema for the
                // best performance, see the documentation:
                //
                //     https://cloud.google.com/bigtable/docs/schema-design
                String rowKey = "greeting" + i;

                // Put a single row into the table. We could also pass a list of Puts to write a batch.
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(GREETINGS[i]));
                table.put(put);
            }*/
            // [END writing_rows]
    //}
}
