package com.umusic.gcp.sst.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.umusic.gcp.sst.speedlayer.data.batch.SSTSpeedLayerAggregationLoader;
import com.umusic.gcp.sst.speedlayer.data.entity.SSTConfiguration;
import com.umusic.gcp.sst.speedlayer.data.util.SSTConsumptionConstants;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerFileUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.regex.Pattern;

/**
 * Created by arumugv on 7/20/17.
 */
public class YamlTesting {

    public static final Logger LOGGER = LoggerFactory.getLogger(YamlTesting.class);

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            User user = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath("user.yaml"), User.class);
            //System.out.println(ReflectionToStringBuilder.toString(user, ToStringStyle.DEFAULT_STYLE.MULTI_LINE_STYLE));

            SSTConfiguration config = mapper.readValue(SSTSpeedLayerFileUtils.loadFilefromClasspath("sst-configuration.yaml"), SSTConfiguration.class);
           // System.out.println(ReflectionToStringBuilder.toString(config, ToStringStyle.DEFAULT_STYLE.MULTI_LINE_STYLE));


            String sql = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.PROJECT_STAGE2_SQLTEMPLATE);

            String sql2 = SSTSpeedLayerFileUtils.loadFilefromClasspath(SSTConsumptionConstants.TOPARTIST_STAGE2_SQLTEMPLATE);

            System.out.println("sql after replace is  " +  config.getQueries().get("changeset-count").get("PROJECT_DAILY"));

            System.out.println("sql is " + sql);

            sql = sql.replace("${TBL_VAR}", "$SST_AGG_DATASET.$TABLE_NAME_PROJ_AGG_STAGING");

            System.out.println("sql after replace is  " + sql);




            System.out.println("sql after replace is  " + sql2.replace("${TBL_VAR}", "$SST_AGG_DATASET.$TABLE_NAME_PROJ_AGG_STAGING"));




        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
