//Not in use currently)
/*
package com.umusic.gcp.sst.speedlayer.data.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

*/
/**
 * Created by arumugv on 2/21/17.
 *//*


public class AvroToMutationParser extends DoFn<GenericRecord, Mutation> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AvroToMutationParser.class);

    //private Set<String> avroFields;

    private String jsonMapping;

    private static final ObjectMapper mapper = new ObjectMapper();



    public AvroToMutationParser(Schema schema, String mappingFile) throws IOException{
        // schema.getF
        jsonMapping = SSTSpeedLayerFileUtils.loadFilefromClasspath(mappingFile);
        //LOGGER.info("json mapping is {}",jsonMapping);
       // System.out.println("json mapping is " + jsonMapping);

    }



    @Override
    public void processElement(ProcessContext c) throws Exception {

        GenericRecord record = c.element();

        JsonNode mappingNode = mapper.readValue(jsonMapping, JsonNode.class);

        JsonNode keyArrNode = mappingNode.findPath("keys");

        //construct key
        StringBuilder key = new StringBuilder();
        if(keyArrNode.isArray()){
            for(int i=0; i < keyArrNode.size(); i++){
                JsonNode node = keyArrNode.get(i);
                String value = "";
                if(node.has("avro-field") && !node.path("avro-field").asBoolean()){
                    value = node.path("name").asText();

                }else{
                    if(record.get(node.path("name").asText()) != null ){
                       // System.out.println("The key = " + node.path("name").asText());
                        value = record.get(node.path("name").asText()).toString();
                       // System.out.print("value = " + value);
                    }

                }

                if(StringUtils.isNotEmpty(value)){

                    //check if we need to reverse the key value
                    if(node.has("reverse") && node.path("reverse").asBoolean()){
                        value = StringUtils.reverse(value);
                    }

                    //check if we need to pad the key value
                    if(node.has("pad")){
                        int padNumber = node.path("pad").asInt();
                        value = StringUtils.leftPad(value, padNumber, '0');
                    }

                    key.append(value);

                    key.append("#");
                   // System.out.println("Aggkey = " + key);
                }


            }

        }
        //LOGGER.info("key is {}",key.toString());
        //LOGGER.info("key is {}", key.toString());
        JsonNode columnArrNode = mappingNode.findPath("columns");

        //construct columns
        if(columnArrNode.isArray()){
            for (JsonNode node : columnArrNode) {
                StringBuilder columnQualifier = new StringBuilder();
                if(node.has("column-qualifier-prefix") &&
                        StringUtils.isNotEmpty(node.path("column-qualifier-prefix").asText())){
                    columnQualifier.append(node.path("column-qualifier-prefix").asText());
                    columnQualifier.append(":");
                }

                //System.out.println(columnQualifier.toString());
                if (node.has("column-qualifier-static") && Boolean.parseBoolean(node.path("column-qualifier-static").asText())) {
                    columnQualifier.append(node.path("column-qualifier").asText().toString());



                } else {
                    //handle multi-part qualifiers

                    String[] arrColumnQualifier = node.path("column-qualifier").asText().split(":");


                    for(int i=0; i < arrColumnQualifier.length; i++) {

                        //handle modifiers
                        //such as rank|pad3 should pad the rang value with 3 zeros

                        //split by | ("pipe") symbol
                        String[] arrModifier = arrColumnQualifier[i].split("\\|");

                        //get the column qualifier value
                        String columnQualifierValue = record.get(arrModifier[0]).toString();

                        //process modifiers. If there is more than one modifier
                        //they will be processed in sequence
                        if (arrModifier.length > 1) {

                            for(int j=1; j < arrModifier.length; j++) {

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
                //LOGGER.info("columnQualifier is {}",columnQualifier.toString());

                //only write if the value is non-zero

                Object value = record.get(node.path("avro-field-name").asText());


                if (value != null && !value.toString().equals("0") && !value.toString().equals("0.0")) {
                    c.output(new Put(key.toString().getBytes()).addColumn(node.path("column-family").asText().getBytes(),
                            columnQualifier.toString().getBytes(),
                            value.toString().getBytes()));
                }

            }
        }


    }


}*/
