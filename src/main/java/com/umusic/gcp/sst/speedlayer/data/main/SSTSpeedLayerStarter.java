package com.umusic.gcp.sst.speedlayer.data.main;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.umusic.gcp.sst.speedlayer.data.batch.GenericSSTSpeedLayerGenerator;
import com.umusic.gcp.sst.speedlayer.data.options.SSTSpeedLayerOptions;
import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by arumugv on 3/27/17.
 * Starter class for SST speed layer
 */
public class SSTSpeedLayerStarter {

    public static final Logger LOGGER = LoggerFactory.getLogger(SSTSpeedLayerStarter.class);


    /**
     * Main method
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //generate SSt speed layer options
        SSTSpeedLayerOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SSTSpeedLayerOptions.class);

        //based on stage BUild or LOAD create a corresponding generator
        GenericSSTSpeedLayerGenerator generator = SSTSpeedLayerUtil.findStage(options.getStage());

        //invoke process.
        generator.initiateProcess(options);


    }



}
