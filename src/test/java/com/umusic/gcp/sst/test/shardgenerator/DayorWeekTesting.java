package com.umusic.gcp.sst.test.shardgenerator;

import com.umusic.gcp.sst.speedlayer.data.util.SSTSpeedLayerUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


/**
 * Created by arumugv on 7/25/17.
 */
public class DayorWeekTesting {

    public static void main(String[] args) {

       System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalDayRange("ALBUM", "19990101"));

     /*   System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalDayRange("TOPARTIST", "20160701", "20160705"));


        System.out.println(SSTSpeedLayerUtil.modifyEltStartDateForDayAgg(LocalDate.parse("20160803", DateTimeFormatter.ofPattern("yyyyMMdd")), "ARTIST"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalDayRange("PROJECT", "20160701", "20160705"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalDayRange("ARTIST", "20160801", "20160831"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalDayRange("PROJECT", "20160701", "20160803"));


        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange("ARTIST", "20160701", "20160705"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange("TOPARTIST", "20160701", "20160705"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange("PROJECT", "20160701", "20160705"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange("ARTIST", "20160801", "20160831"));

        System.out.println(SSTSpeedLayerUtil.isDatesWithinGlobalWeekRange("PROJECT", "20160701", "20160803"));

        */
    }
}
