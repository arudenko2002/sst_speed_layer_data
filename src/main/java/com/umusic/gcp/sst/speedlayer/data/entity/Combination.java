package com.umusic.gcp.sst.speedlayer.data.entity;

import com.umusic.gcp.sst.speedlayer.data.enumtype.*;

/**
 * Created by arumugv on 3/21/17.
 * Entity class for combination
 */
public class Combination {

    private PeriodType period;

    private Territory territory;

    private TerritorySubDivision territorysubdivision;

    private Partner partner;

    private Label label;

    private Genre projectGenre;

    private Genre trackGenre;

    private UsageType usagetype;

    private Catalog projectCatalog;

    private Catalog trackCatalog;

    public PeriodType getPeriod() {
        return period;
    }

    public void setPeriod(PeriodType period) {
        this.period = period;
    }

    public Territory getTerritory() {
        return territory;
    }

    public void setTerritory(Territory territory) {
        this.territory = territory;
    }

    public Partner getPartner() {
        return partner;
    }

    public void setPartner(Partner partner) {
        this.partner = partner;
    }

    public Label getLabel() {
        return label;
    }

    public void setLabel(Label label) {
        this.label = label;
    }



    public UsageType getUsagetype() {
        return usagetype;
    }

    public void setUsagetype(UsageType usagetype) {
        this.usagetype = usagetype;
    }

    public TerritorySubDivision getTerritorysubdivision() {
        return territorysubdivision;
    }

    public void setTerritorysubdivision(TerritorySubDivision territorysubdivision) {
        this.territorysubdivision = territorysubdivision;
    }

    public Genre getProjectGenre() {
        return projectGenre;
    }

    public void setProjectGenre(Genre projectGenre) {
        this.projectGenre = projectGenre;
    }

    public Genre getTrackGenre() {
        return trackGenre;
    }

    public void setTrackGenre(Genre trackGenre) {
        this.trackGenre = trackGenre;
    }

    public Catalog getProjectCatalog() {
        return projectCatalog;
    }

    public void setProjectCatalog(Catalog projectCatalog) {
        this.projectCatalog = projectCatalog;
    }

    public Catalog getTrackCatalog() {
        return trackCatalog;
    }

    public void setTrackCatalog(Catalog trackCatalog) {
        this.trackCatalog = trackCatalog;
    }
}
