
package com.umusic.gcp.sst.speedlayer.data.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.umusic.gcp.sst.speedlayer.data.enumtype.DataLoadingType;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by arumugv on 2/21/17.
 * Parser to add mutuations to BT
 */


public class TableRowToPutMutationParser extends DoFn<TableRow, Mutation> {

    public static final Logger LOGGER = LoggerFactory.getLogger(TableRowToPutMutationParser.class);


    //private String jsonMapping;

    private Map<Integer, Map<DataLoadingType, String>> loadingSchema;

    private Map<String, Map<String, List<String>>> lookUp;

    private static final ObjectMapper mapper = new ObjectMapper();


    private List<String> generateSchemabasedKey(DataLoadingType type, String key, TableRow record) {
        Map<String, List<String>> countryRegionlkup = lookUp.get("country-region");
        List<String> keyList = new ArrayList<>();
        //list.
        if (type.equals(DataLoadingType.BY_COUNTRY_ALL_REGION)) {
            keyList.add(key.replace("${territory_marker}", "A").replace("${territory}", "ALL"));

        } else if (type.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_COUNTRY))
        {
            keyList.add(key.replace("${territory_marker}", "C").replace("${territory}", record.get("territory").toString()));

        } else if (type.equals(DataLoadingType.BY_COUNTRY_SPECIFIC_REGION)) {
            List<String> regionLst = countryRegionlkup.get(record.get("territory"));
            if (regionLst != null && regionLst.size() > 0) {
                for (String region : regionLst) {
                    keyList.add(key.replace("${territory_marker}", "R").replace("${territory}", region));
                }
            }

        } else {
            keyList.add(key);
        }
        return keyList;
    }


    public TableRowToPutMutationParser(Map<Integer, Map<DataLoadingType, String>> loadingSchema, Map<String, Map<String, List<String>>> lookUp) throws IOException {
        this.loadingSchema = loadingSchema;
        this.lookUp = lookUp;


    }


    @Override
    public void processElement(ProcessContext c) throws Exception {

        TableRow record = c.element();

        //get shardnumber
        Integer shardNumber = Integer.valueOf(record.get("shard_number").toString());
        //get loading schema for shard specific
        Map<DataLoadingType, String> loadingSchemaShrd = loadingSchema.get(shardNumber);

        //load based on all dataloadingtypes
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

            JsonNode columnArrNode = mappingNode.findPath("columns");

            //construct columns
            if (columnArrNode.isArray()) {
                for (JsonNode node : columnArrNode) {
                    StringBuilder columnQualifier = new StringBuilder();
                    if (node.has("column-qualifier-prefix") &&
                            StringUtils.isNotEmpty(node.path("column-qualifier-prefix").asText())) {
                        columnQualifier.append(node.path("column-qualifier-prefix").asText());
                        columnQualifier.append(":");
                    }


                    if (node.has("column-qualifier-static") && Boolean.parseBoolean(node.path("column-qualifier-static").asText())) {
                        columnQualifier.append(node.path("column-qualifier").asText().toString());


                    } else {
                        //handle multi-part qualifiers

                        String[] arrColumnQualifier = node.path("column-qualifier").asText().split(":");


                        for (int i = 0; i < arrColumnQualifier.length; i++) {

                            //handle modifiers
                            //such as rank|pad3 should pad the rang value with 3 zeros

                            //split by | ("pipe") symbol
                            String[] arrModifier = arrColumnQualifier[i].split("\\|");


                            String columnQualifierValue = record.get(arrModifier[0]) != null ? record.get(arrModifier[0]).toString() : "";

                            //process modifiers. If there is more than one modifier
                            //they will be processed in sequence
                            if (arrModifier.length > 1) {

                                for (int j = 1; j < arrModifier.length; j++) {

                                    if (arrModifier[j].contains("pad")) {

                                        //remove "pad" from the string, leaving number of characters to pad
                                        int padNumber = Integer.parseInt(arrModifier[j].replace("pad", ""));

                                        columnQualifierValue = StringUtils.leftPad(columnQualifierValue, padNumber, '0');


                                    }

                                    //add here processing of additional modifiers in the future if needed

                                }

                            }


                            columnQualifier.append(columnQualifierValue);

                            if (i != arrColumnQualifier.length - 1) {

                                columnQualifier.append(":");
                            }
                        }

                    }


                    //only write if the value is non-zero

                    Object value = record.get(node.path("field-name").asText());


                    if (value != null && !value.toString().equals("0") && !value.toString().equals("0.0")) {
                        List<String> keyList = generateSchemabasedKey(type, key.toString(), record);
                        for (String newKey : keyList) {
                            try {
                                LOGGER.debug("CustomMsg: loading type {} Error while ingesting {} for column family {} with value {}",
                                        type, newKey, node.path("column-family"), value);
                                c.output(new Put(newKey.getBytes()).addColumn(node.path("column-family").asText().getBytes(),
                                        columnQualifier.toString().getBytes(),
                                        value.toString().getBytes()));

                            } catch (Exception e) {
                                LOGGER.error("CustomMsg: loading type {} Error while ingesting {} for column family {} with value {}  - error message is {}",
                                        type, newKey, node.path("column-family"), value, e.getMessage());
                                e.printStackTrace();
                                throw new Exception("type=" + type + "key = " + newKey + " column-family=" + node.path("column-family") + " column-qualifier=" + columnQualifier
                                        + " value=" + value + e.getMessage() + e);
                            }
                        }

                    }

                }
            }


        }


    }


}
