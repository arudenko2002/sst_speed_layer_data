package com.umusic.gcp.sst.speedlayer.data.exception;

/**
 * Created by arumugv on 5/19/17.
 * Custom Exception Class for SST speed layer
 */
public class SSTSpeedLayerException extends Exception {

    /**
     * Constructor with msg and e
     * @param msg
     * @param e
     */
    public SSTSpeedLayerException(String msg, Exception e){
        super(msg,e);
    }

    /**
     * Constructor with msg
     * @param msg
     */
    public SSTSpeedLayerException(String msg){
        super(msg);
    }

}
