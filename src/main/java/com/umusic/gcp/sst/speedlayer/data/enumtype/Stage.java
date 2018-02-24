package com.umusic.gcp.sst.speedlayer.data.enumtype;

/**
 * Created by arumugv on 3/27/17.
 */
public enum Stage {

    BUILD,
    LOAD,

    CHANGESETBUILD,
    CHANGESETLOAD,
    CHANGESETUNLOAD,

    //This is stage is not used anymore, removing the processed(scheduled) dates using sql in airflow job//This is stage is not used anymore, removing the processed(scheduled) dates using sql in airflow job
    // /REMOVEPROCESSEDDATE;

}
