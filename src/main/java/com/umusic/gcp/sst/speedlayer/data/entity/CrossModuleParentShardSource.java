package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;

/**
 * Created by arumugv on 10/6/17.
 */
public class CrossModuleParentShardSource {

    private int tpShardCode;

    private PeriodType tpPeriod;

    private int lptdShardCode;

    private PeriodType lptdPeriod;


    public int getTpShardCode() {
        return tpShardCode;
    }

    public void setTpShardCode(int tpShardCode) {
        this.tpShardCode = tpShardCode;
    }

    public PeriodType getTpPeriod() {
        return tpPeriod;
    }

    public void setTpPeriod(PeriodType tpPeriod) {
        this.tpPeriod = tpPeriod;
    }

    public int getLptdShardCode() {
        return lptdShardCode;
    }

    public void setLptdShardCode(int lptdShardCode) {
        this.lptdShardCode = lptdShardCode;
    }

    public PeriodType getLptdPeriod() {
        return lptdPeriod;
    }


    public void setLptdPeriod(PeriodType lptdPeriod) {
        this.lptdPeriod = lptdPeriod;
    }


}
