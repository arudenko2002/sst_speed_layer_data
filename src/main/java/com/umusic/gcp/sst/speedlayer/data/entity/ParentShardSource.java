package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.PeriodType;

/**
 * Created by arumugv on 3/24/17.
 * Entity class to map parentshard source from shard config file
 */
public class ParentShardSource {
    public int getShardcode() {
        return shardcode;
    }

    public void setShardcode(int shardcode) {
        this.shardcode = shardcode;
    }

    public PeriodType getPeriod() {
        return period;
    }

    public void setPeriod(PeriodType period) {
        this.period = period;
    }

    private int shardcode;

    private PeriodType period;


}
