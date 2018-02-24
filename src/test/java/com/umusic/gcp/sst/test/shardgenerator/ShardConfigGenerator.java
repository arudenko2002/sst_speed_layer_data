package com.umusic.gcp.sst.test.shardgenerator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.umusic.gcp.sst.speedlayer.data.entity.Combination;
import com.umusic.gcp.sst.speedlayer.data.entity.Dimension;
import com.umusic.gcp.sst.speedlayer.data.entity.ParentShardSource;
import com.umusic.gcp.sst.speedlayer.data.enumtype.*;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by arumugv on 6/15/17.
 */
public class ShardConfigGenerator {


    private static final ObjectMapper mapper = new ObjectMapper();

    private static JsonNode configNode;

    private static int startRow;

    private static Map<Integer, Dimension> dimensions = new TreeMap<Integer, Dimension>();

    public static void buildPeriod(Combination comb, String[] words){

        JsonNode periodNode = configNode.path("period");

        Iterator<String> filedItr =  periodNode.fieldNames();

        while(filedItr.hasNext()){

           String field =  filedItr.next();

           if(words[periodNode.path(field).asInt()].equals("x")){

               comb.setPeriod(PeriodType.valueOf(field));

           }
        }
    }

    public static void buildPartner(Combination comb, String[] words){
        JsonNode node = configNode.path("partner");
        if(node.asInt() < words.length &&  words[node.asInt()].equals("x")){
            comb.setPartner(Partner.Y);
        }else{
            comb.setPartner(Partner.ALL);
        }

    }

    public static void buildprojectGenre(Combination comb, String[] words){
        JsonNode node = configNode.path("projectGenre");
        if(node.asInt() < words.length && words[node.asInt()].equals("x")){
            comb.setProjectGenre(Genre.Y);
        }else{
            comb.setProjectGenre(Genre.ALL);
        }

    }

    public static void buildCatalogue(Combination comb, String[] words) throws Exception{

        try {
            JsonNode node = configNode.path("catalogue");
            if (node.asInt() < words.length && words[node.asInt()].equals("x")) {
                comb.setProjectCatalog(Catalog.Y);
            } else {
                comb.setProjectCatalog(Catalog.ALL);
            }
        }
        catch(Exception e){
            for(String wd : words){
                System.out.println(wd);
            }

            e.printStackTrace();
            throw new Exception();
        }

    }

    public static void buildTerritory(Combination comb, String[] words){

        JsonNode node = configNode.path("territory");

        Iterator<String> filedItr =  node.fieldNames();

        boolean marked = false;

        while(filedItr.hasNext()){

            String field =  filedItr.next();

            if(words[node.path(field).asInt()].equals("x")){

                comb.setTerritory(Territory.valueOf(field));
                marked = true;

            }
        }

        if(!marked){
            comb.setTerritory(Territory.ALL);
        }
    }


    public static void buildLabel(Combination comb, String[] words){

        JsonNode node = configNode.path("label");

        Iterator<String> filedItr =  node.fieldNames();
        boolean marked = false;
        while(filedItr.hasNext()){

            String field =  filedItr.next();

            if(words[node.path(field).asInt()].equalsIgnoreCase("x")){

                comb.setLabel(Label.valueOf(field));
                marked = true;
            }
        }

        if(!marked){
            comb.setLabel(Label.ALL);
        }
    }



    public static void loadConfigFile(String configPath) throws Exception{
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        configNode = mapper.readValue( SSTSpeedLayerFileUtils.loadFilefromClasspath(configPath), JsonNode.class);
        startRow = configNode.has("startrow") == true ? configNode.path("startrow").asInt() : 0;
        System.out.println(configNode.toString());

    }

    public static void buildparentShard(Dimension dm, String[] words){
        JsonNode node = configNode.path("parentshardcode");
        if(node.asInt() < words.length &&  StringUtils.isNotEmpty(words[node.asInt()])){
            int parentShard = Integer.parseInt(words[node.asInt()]);
            Dimension parent = dimensions.get(parentShard);
            ParentShardSource obj = new ParentShardSource();
            obj.setShardcode(parent.getShardcode());
            obj.setPeriod(parent.getCombination().getPeriod());
            dm.setParentShardSource(obj);
        }
    }


    public static void processFile(String path) throws Exception{

        List<String> list = null;

        try (Stream<String> stream = Files.lines(Paths.get(path))) {


            list = stream
                    //.filter(line -> !line.startsWith("line3"))
                    // .map(String::toUpperCase)
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i=0; i < list.size(); i++) {
            String line = list.get(i);


            if(i >= startRow){
                if(!line.contains("\t")){
                  continue;
                }
                String[] words = line.split("\t");
                //for (String wd : words){
                int shardCd = Integer.parseInt(words[configNode.path("shardcode").asInt()]);

                Dimension dm = new Dimension();
                dm.setShardcode(shardCd);
                Combination cmb = new Combination();
                dm.setCombination(cmb);



                Iterator<String> filedItr = configNode.fieldNames();
                while(filedItr.hasNext()) {

                    String field = filedItr.next();
                    if(field.equals("period")){
                        buildPeriod(cmb, words);
                    }else if(field.equals("territory")){
                        buildTerritory(cmb, words);
                    }else if(field.equals("label")){
                        buildLabel(cmb, words);
                    }else if(field.equals("partner")){
                        buildPartner(cmb, words);
                    }else if(field.equals("projectGenre")){
                        buildprojectGenre(cmb, words);
                    }else if(field.equals("catalogue")){
                        buildCatalogue(cmb, words);
                    }else if(field.equals("parentshardcode")){
                       buildparentShard(dm, words);
                    }
                }

                dimensions.put(shardCd, dm);


                //}

            }


        }

    }


    public static void main(String[] args) throws Exception{
        //
        ShardConfigGenerator.loadConfigFile("shardmap.json");

        ShardConfigGenerator.processFile("/Users/arumugv/Downloads/Consumption_Speed_Layer_Aggregations_top_project.tsv");

        List<Dimension> ls = new ArrayList<Dimension>();
        for (Integer key :dimensions.keySet()) {
            ls.add(dimensions.get(key));
        }
       //SortedList list =  dimensions.values();
        Path path = Paths.get("/Users/arumugv/Documents/output_shard.json");

//Use try-with-resource to get auto-closeable writer instance
        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(dimensions.values()));
        }

       // System.out.println(mapper.writeValueAsString(dimensions.values()));

    }
}
