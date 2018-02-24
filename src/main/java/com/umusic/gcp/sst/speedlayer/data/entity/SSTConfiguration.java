package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.Module;

import java.util.Map;
import java.util.Set;

/**
 * Created by arumugv on 7/20/17.
 */
public class SSTConfiguration {



    /**
     * inner entity class for week/day configuration
     */
    public class WeekOrDayShard{

        Set<Module> modules;

        String weekStartDate;

        String weekEndDate;

        String dayStartDate;

        String dayEndDate;

        public Set<Module> getModules() {
            return modules;
        }

        public void setModules(Set<Module> modules) {
            this.modules = modules;
        }

        public String getWeekStartDate() {
            return weekStartDate;
        }

        public void setWeekStartDate(String weekStartDate) {
            this.weekStartDate = weekStartDate;
        }

        public String getWeekEndDate() {
            return weekEndDate;
        }

        public void setWeekEndDate(String weekEndDate) {
            this.weekEndDate = weekEndDate;
        }

        public String getDayStartDate() {
            return dayStartDate;
        }

        public void setDayStartDate(String dayStartDate) {
            this.dayStartDate = dayStartDate;
        }

        public String getDayEndDate() {
            return dayEndDate;
        }

        public void setDayEndDate(String dayEndDate) {
            this.dayEndDate = dayEndDate;
        }
    }

    private String rtdStartDate;

    private String rtdEndDate;

    private Map<String, Map<String, String>> queries;

    private Map<String, String> datasetName;

    private Map<String, String> tableName;



    private Map<String, String> tableAlias;

    private boolean weekForPriorPeriod;

    private boolean crossModuleBuild;

    private boolean decisionEngineEnabled;


    private WeekOrDayShard weekOrDayShard;


    public Map<String, Map<String, String>> getQueries() {
        return queries;

    }

    public void setQueries(Map<String, Map<String, String>> queries) {
        this.queries = queries;
    }

    public Map<String, String> getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(Map<String, String> datasetName) {
        this.datasetName = datasetName;
    }

    public Map<String, String> getTableName() {
        return tableName;
    }

    public void setTableName(Map<String, String> tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(Map<String, String> tableAlias) {
        this.tableAlias = tableAlias;
    }



    public WeekOrDayShard getWeekOrDayShard() {
        return weekOrDayShard;
    }

    public void setWeekOrDayShard(WeekOrDayShard weekOrDayShard) {
        this.weekOrDayShard = weekOrDayShard;
    }

    public boolean isWeekForPriorPeriod() {
        return weekForPriorPeriod;
    }

    public String getRtdStartDate() {
        return rtdStartDate;
    }

    public void setRtdStartDate(String rtdStartDate) {
        this.rtdStartDate = rtdStartDate;
    }

    public String getRtdEndDate() {
        return rtdEndDate;
    }

    public void setRtdEndDate(String rtdEndDate) {
        this.rtdEndDate = rtdEndDate;
    }

    public boolean isCrossModuleBuild() {
        return crossModuleBuild;
    }

    public void setCrossModuleBuild(boolean crossModuleBuild) {
        this.crossModuleBuild = crossModuleBuild;
    }

    public void setWeekForPriorPeriod(boolean weekForPriorPeriod) {
        this.weekForPriorPeriod = weekForPriorPeriod;
    }


    public boolean isDecisionEngineEnabled() {
        return decisionEngineEnabled;
    }

    public void setDecisionEngineEnabled(boolean decisionEngineEnabled) {
        this.decisionEngineEnabled = decisionEngineEnabled;
    }


}
