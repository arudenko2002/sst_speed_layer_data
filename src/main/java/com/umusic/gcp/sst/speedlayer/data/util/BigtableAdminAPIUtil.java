package com.umusic.gcp.sst.speedlayer.data.util;

import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableConfiguration;
import com.umusic.gcp.sst.speedlayer.data.entity.BTTableEntity;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;


import java.io.IOException;

/**
 * Created by arumugv on 9/6/17.
 * class to perform bigtable check table exists and create if not exists etc
 */
public class BigtableAdminAPIUtil {



    private Connection connection;

    public BigtableAdminAPIUtil(String projectId, String instanceId){
        connection = BigtableConfiguration.connect(projectId, instanceId);
    }


    /**
     * check BT table exists
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean checkTableExists(String tableName) throws IOException{

        Admin admin = connection.getAdmin();

        if(!admin.tableExists(TableName.valueOf(tableName))){
            return false;
        }
        return true;
    }

    /**
     * create BT table with column families
     * @param tableName
     * @param schema
     * @return
     * @throws IOException
     */
    public boolean createTable(String tableName, BTTableEntity schema) throws IOException{

        Admin admin = connection.getAdmin();

        if(admin.tableExists(TableName.valueOf(tableName))){
            return true;
        }
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String column : schema.getColumnFamily()) {
            descriptor.addFamily(new HColumnDescriptor(column));
        }
        admin.createTable(descriptor);
        return true;
    }

    /**
     * close connection
     * @throws IOException
     */
    public void closeConnection() throws IOException{
        if(connection != null){
            connection.close();
        }
    }


}
