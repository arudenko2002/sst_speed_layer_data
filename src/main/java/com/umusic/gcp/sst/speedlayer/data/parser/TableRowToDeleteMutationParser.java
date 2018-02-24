
package com.umusic.gcp.sst.speedlayer.data.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.exception.SSTSpeedLayerException;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * Created by arumugv on 2/21/17.
 * Parser to delete mutation from BT
 */


public class TableRowToDeleteMutationParser extends DoFn<TableRow, Mutation> {

    public static final Logger LOGGER = LoggerFactory.getLogger(TableRowToDeleteMutationParser.class);


    private Map<Integer, Map<DataLoadingType, String>> loadingSchema;

    private static final ObjectMapper mapper = new ObjectMapper();


    public TableRowToDeleteMutationParser(Map<Integer, Map<DataLoadingType, String>> loadingSchema) throws IOException {
        this.loadingSchema = loadingSchema;
    }


    @Override
    public void processElement(ProcessContext c) throws Exception {

        TableRow record = c.element();

        //get shardnumber
        Integer shardNumber = Integer.valueOf(record.get("shard_number").toString());
        //get loading schema for shard specific
        Map<DataLoadingType, String> loadingSchemaShrd = loadingSchema.get(shardNumber);

        for (DataLoadingType type : loadingSchemaShrd.keySet()) {

            JsonNode mappingNode = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath(loadingSchemaShrd.get(type)), JsonNode.class);

            JsonNode keyArrNode = mappingNode.findPath("keys");

            //construct key
            StringBuilder key = new StringBuilder();
            if (keyArrNode.isArray()) {
                for (int i = 0; i < keyArrNode.size(); i++) {
                    JsonNode node = keyArrNode.get(i);
                    String value = "";
                    if (node.has("field") && !node.path("field").asBoolean()) {
                        value = node.path("name").asText();

                    } else {
                        if (record.get(node.path("name").asText()) != null) {
                            value = record.get(node.path("name").asText()).toString();
                        }

                    }

                    if (StringUtils.isNotEmpty(value)) {
                        //check if we need to reverse the key value
                        if (node.has("reverse") && node.path("reverse").asBoolean()) {
                            value = StringUtils.reverse(value);
                        }

                        //check if we need to pad the key value
                        if (node.has("pad")) {
                            int padNumber = node.path("pad").asInt();
                            value = StringUtils.leftPad(value, padNumber, '0');
                        }
                        key.append(value);
                        key.append("#");
                    }
                }
            }


            try {
                LOGGER.debug(" The delete key is {}", key.toString());
                c.output(new Delete(key.toString().getBytes()));

            } catch (Exception e) {
                LOGGER.error("CustomMsg: Error while deleting key {} ",
                        key);
                throw new SSTSpeedLayerException("key = " + key + e.getMessage() + e);
            }

        }


    }


}
