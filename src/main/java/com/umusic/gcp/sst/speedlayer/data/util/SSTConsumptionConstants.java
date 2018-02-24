package com.umusic.gcp.sst.speedlayer.data.util;

/**
 * Constants CLASS FOR SST sSpeed layer Consumption,
 */
public class SSTConsumptionConstants {


    public static String SOURCE_TABLE_CONFIG = "aggregation-build/source-table-mapping.json";

    //Top Artists configs

    public static String TOPARTIST_AGGREGATION_TABLE_CONFIG = "aggregation-build/top-artist/top-artist-schema.json";

    //shard config
    public static String TOPARTIST_SHARD_CONFIG = "shard/top-artist-shard.json";

    public static String TOPARTIST_BIGTABLE_CONFIG = "aggregation-load/top-artist/mapping/top-artist-bigtable.json";

    //sql template config
    public static String TOPARTIST_STAGE1_SQLTEMPLATE = "aggregation-build/top-artist/sql/top-artist.sql";

    public static String TOPARTIST_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-artist/sql/top-artist-parentshard.sql";

    public static String TOPARTIST_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-artist/sql/top-artist-crossmodule-parentshard.sql";

    public static String TOPARTIST_STAGE2_SQLTEMPLATE = "aggregation-load/top-artist/sql/top-artist.sql";

    public static String TOPARTIST_BT_TABLE_SCHEMA = "aggregation-load/top-artist/table-schema/top-artist-schema.json";


    //Artist detail config

    public static String ARTIST_AGGREGATION_TABLE_CONFIG = "aggregation-build/artist/table-schema/artist-schema.json";

    public static String ARTIST_SHARD_CONFIG = "shard/artist-shard.json";

    public static String ARTIST_BIGTABLE_DEFAULT_CONFIG = "aggregation-load/artist/mapping/artist.json";

    public static String ARTIST_BIGTABLE_BYPARTNER_CONFIG = "aggregation-load/artist/mapping/artist_by_partner.json";

    public static String ARTIST_BIGTABLE_BYPARTNER_SPECIFIC_CONFIG = "aggregation-load/artist/mapping/artist_by_partner_specific_partner.json";

    public static String ARTIST_BIGTABLE_BYCOUNTRY_CONFIG = "aggregation-load/artist/mapping/artist_by_country.json";

    public static String ARTIST_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG = "aggregation-load/artist/mapping/artist_by_country_subdivision.json";

    public static String ARTIST_STAGE1_SQLTEMPLATE = "aggregation-build/artist/sql/artist.sql";

    public static String ARTIST_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/artist/sql/artist-parentshard.sql";

    public static String ARTIST_STAGE2_SQLTEMPLATE = "aggregation-load/artist/sql/artist.sql";

    public static String ARTIST_BT_TABLE_SCHEMA = "aggregation-load/artist/table-schema/artist-schema.json";


    //Track detail config
    //aggregation table config
    public static String TRACK_AGGREGATION_TABLE_CONFIG = "aggregation-build/track/table-schema/track-schema.json";

    //shard config
    public static String TRACK_SHARD_CONFIG = "shard/track-shard.json";

    //aggregation sql template
    public static String TRACK_STAGE1_SQLTEMPLATE = "aggregation-build/track/sql/track.sql";

    public static String TRACK_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/track/sql/track-parentshard.sql";

    //loading sql temaplte
    public static String TRACK_STAGE2_SQLTEMPLATE = "aggregation-load/track/sql/track.sql";

    //BT loading mapping config
    public static String TRACK_BIGTABLE_DEFAULT_CONFIG = "aggregation-load/track/mapping/track.json";

    public static String TRACK_BIGTABLE_BYPARTNER_CONFIG = "aggregation-load/track/mapping/track_by_partner.json";

    public static String TRACK_BIGTABLE_BYCOUNTRY_CONFIG = "aggregation-load/track/mapping/track_by_country.json";

    public static String TRACK_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG = "aggregation-load/track/mapping/track_by_country_subdivision.json";

    public static String TRACK_BIGTABLE_USAGETYPE_TIER_CONFIG = "aggregation-load/track/mapping/track_by_usagetype_tier.json";

    public static String TRACK_BIGTABLE_PARTNER_USAGETYPE_TIER_CONFIG = "aggregation-load/track/mapping/track_by_usagetype_tier_all_partner.json";

    public static String TRACK_BT_TABLE_SCHEMA = "aggregation-load/track/table-schema/track-schema.json";


    //Top tracks configs

    //aggregation table config
    public static String TOPTRACK_AGGREGATION_TABLE_CONFIG = "aggregation-build/top-track/top-track-schema.json";

    //shard config
    public static String TOPTRACK_SHARD_CONFIG = "shard/top-track-shard.json";

    //BT loading mapping config
    public static String TOPTRACK_BIGTABLE_CONFIG = "aggregation-load/top-track/mapping/top-track-bigtable.json";

    //aggregation sql template
    public static String TOPTRACK_STAGE1_SQLTEMPLATE = "aggregation-build/top-track/sql/top-track.sql";

    public static String TOPTRACK_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-track/sql/top-track-parentshard.sql";

    public static String TOPTRACK_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-track/sql/top-track-crossmodule-parentshard.sql";

    //loading sql temaplte
    public static String TOPTRACK_STAGE2_SQLTEMPLATE = "aggregation-load/top-track/sql/top-track.sql";

    public static String TOPTRACK_BT_TABLE_SCHEMA = "aggregation-load/top-track/table-schema/top-track-schema.json";


    //Top project configs

    //aggregation table config
    public static String TOPPROJECT_AGGREGATION_TABLE_CONFIG = "aggregation-build/top-project/top-project-schema.json";

    //shard config
    public static String TOPPROJECT_SHARD_CONFIG = "shard/top-project-shard.json";

    //BT loading mapping config
    public static String TOPPROJECT_BIGTABLE_CONFIG = "aggregation-load/top-project/mapping/top-project-bigtable.json";

    //aggregation sql template
    public static String TOPPROJECT_STAGE1_SQLTEMPLATE = "aggregation-build/top-project/sql/top-project.sql";

    public static String TOPPROJECT_CROSSMODULE_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-project/sql/top-project-crossmodule-parentshard.sql";

    public static String TOPPROJECT_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/top-project/sql/top-project-parentshard.sql";

    //loading sql temaplte
    public static String TOPPROJECT_STAGE2_SQLTEMPLATE = "aggregation-load/top-project/sql/top-project.sql";

    public static String TOPPROJECT_BT_TABLE_SCHEMA = "aggregation-load/top-project/table-schema/top-project-schema.json";


    //PROJECT detail config

    //aggregation table config
    public static String PROJECT_AGGREGATION_TABLE_CONFIG = "aggregation-build/project/table-schema/project-schema.json";

    //shard config
    public static String PROJECT_SHARD_CONFIG = "shard/project-shard.json";

    //aggregation sql template
    public static String PROJECT_STAGE1_SQLTEMPLATE = "aggregation-build/project/sql/project.sql";

    public static String PROJECT_PARENT_SHARD_SQLTEMPLATE = "aggregation-build/project/sql/project-parentshard.sql";

    //loading sql temaplte
    public static String PROJECT_STAGE2_SQLTEMPLATE = "aggregation-load/project/sql/project.sql";

    //BT loading mapping config
    public static String PROJECT_BIGTABLE_DEFAULT_CONFIG = "aggregation-load/project/mapping/project.json";

    public static String PROJECT_BIGTABLE_BYPARTNER_CONFIG = "aggregation-load/project/mapping/project_by_partner.json";

    public static String PROJECT_BIGTABLE_BYPARTNER_SPECIFIC_CONFIG = "aggregation-load/project/mapping/project_by_partner_specific_partner.json";

    public static String PROJECT_BIGTABLE_BYCOUNTRY_CONFIG = "aggregation-load/project/mapping/project_by_country.json";

    public static String PROJECT_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG = "aggregation-load/project/mapping/project_by_country_subdivision.json";

    public static String PROJECT_BT_TABLE_SCHEMA = "aggregation-load/project/table-schema/project-schema.json";


    //ISRC detail config

    //aggregation table config
    public static String ISRC_AGGREGATION_TABLE_CONFIG = "aggregation-build/isrc/table-schema/isrc-schema.json";

    //shard config
    public static String ISRC_SHARD_CONFIG = "shard/isrc-shard.json";

    //aggregation sql template
    public static String ISRC_AGG_SQLTEMPLATE = "aggregation-build/isrc/sql/isrc.sql";

    public static String ISRC_AGG_PARENTSHARD_SQLTEMPLATE = "aggregation-build/isrc/sql/isrc-parentshard.sql";

    //loading sql temaplte
    public static String ISRC_LOAD_SQLTEMPLATE = "aggregation-load/isrc/sql/isrc.sql";

    //BT loading mapping config
    public static String ISRC_LOADING_DEFAULT_MAPPING = "aggregation-load/isrc/mapping/isrc.json";

    //BT loading mapping config
    public static String ISRC_BIGTABLE_BYPARTNER_CONFIG = "aggregation-load/isrc/mapping/isrc_by_partner.json";

    public static String ISRC_BIGTABLE_BYCOUNTRY_CONFIG = "aggregation-load/isrc/mapping/isrc_by_country.json";

    public static String ISRC_BIGTABLE_BYCOUNTRY_SUBDIVISION_CONFIG = "aggregation-load/isrc/mapping/isrc_by_country_subdivision.json";

    public static String ISRC_BIGTABLE_USAGETYPE_TIER_CONFIG = "aggregation-load/isrc/mapping/isrc_by_usagetype_tier.json";

    public static String ISRC_BIGTABLE_PARTNER_USAGETYPE_TIER_CONFIG = "aggregation-load/isrc/mapping/isrc_by_usagetype_tier_all_partner.json";

    public static String ISRC_BT_TABLE_SCHEMA = "aggregation-load/isrc/table-schema/isrc-schema.json";


    //ALBUM detail config

    //aggregation table config
    public static String ALBUM_AGGREGATION_TABLE_CONFIG = "aggregation-build/album/table-schema/album-schema.json";

    //shard config
    public static String ALBUM_SHARD_CONFIG = "shard/album-shard.json";

    //aggregation sql template
    public static String ALBUM_AGG_SQLTEMPLATE = "aggregation-build/album/sql/album.sql";

    public static String ALBUM_AGG_PARENTSHARD_SQLTEMPLATE = "aggregation-build/album/sql/album-parentshard.sql";

    //loading sql temaplte
    public static String ALBUM_LOAD_SQLTEMPLATE = "aggregation-load/album/sql/album.sql";

    //BT loading mapping config
    public static String ALBUM_LOADING_DEFAULT_MAPPING = "aggregation-load/album/mapping/album.json";

    public static String ALBUM_BT_TABLE_SCHEMA = "aggregation-load/album/table-schema/album-schema.json";


    //changeset config
    //track changeset

    public static String TRACK_CHANGESET_CONFIG_FILE = "aggregation-changeset/track/changeset.json";

    public static String TRACK_ALTERNATE_DELETE_SQL = "aggregation-changeset/track/sql/track-delete.sql";

    public static String TRACK_UNLOAD_DELETE_SQL = "aggregation-changeset/track/sql/track-unload.sql";

    public static String PROJECT_CHANGESET_CONFIG_FILE = "aggregation-changeset/project/changeset.json";

    public static String PROJECT_ALTERNATE_DELETE_SQL = "aggregation-changeset/project/sql/project-delete.sql";

    public static String PROJECT_UNLOAD_DELETE_SQL = "aggregation-changeset/project/sql/project-unload.sql";

    public static String ARTIST_CHANGESET_CONFIG_FILE = "aggregation-changeset/artist/changeset.json";

    public static String ARTIST_ALTERNATE_DELETE_SQL = "aggregation-changeset/artist/sql/artist-delete.sql";

    public static String ARTIST_UNLOAD_DELETE_SQL = "aggregation-changeset/artist/sql/artist-unload.sql";

    public static String REGION_CHANGESET_CONFIG_FILE = "aggregation-changeset/region/changeset.json";


    //config related constants
    public static String SST_CONFIGURATION_FILENAME = "sst-configuration.yaml";

    public static String SST_CONFIG_LKUP_DAY_DISTINCT = "DAY-DISTINCT";

    public static String SST_CONFIG_LKUP_DAY_DISTINCT_DYNAMICENGINE = "DAY-DISTINCT-DYNAMICENGINE";

    public static String SST_CONFIG_LKUP_DAY_RANGE = "DAY-RANGE";

    public static String SST_CONFIG_LKUP_WEEK_DISTINCT = "WEEK-DISTINCT";

    public static String SST_CONFIG_LKUP_WEEK_DISTINCT_DYNAMICENGINE = "WEEK-DISTINCT-DYNAMICENGINE";

    public static String SST_CONFIG_LKUP_WEEK_RANGE = "WEEK-RANGE";


    public static String SST_CONFIG_LKUP_QUARTER_DISTINCT = "QUARTER-DISTINCT";

    public static String SST_CONFIG_LKUP_QUARTER_DISTINCT_DYNAMICENGINE = "QUARTER-DISTINCT-DYNAMICENGINE";

    public static String SST_CONFIG_LKUP_QUARTER_RANGE = "QUARTER-RANGE";


    public static String SST_CONFIG_LKUP_MONTH_DISTINCT = "MONTH-DISTINCT";

    public static String SST_CONFIG_LKUP_MONTH_DISTINCT_DYNAMICENGINE = "MONTH-DISTINCT-DYNAMICENGINE";

    public static String SST_CONFIG_LKUP_MONTH_RANGE = "MONTH-RANGE";


    public static String SST_CONFIG_LKUP_YEAR_DISTINCT = "YEAR-DISTINCT";

    public static String SST_CONFIG_LKUP_YEAR_DISTINCT_DYNAMICENGINE = "YEAR-DISTINCT-DYNAMICENGINE";

    public static String SST_CONFIG_LKUP_YEAR_RANGE = "YEAR-RANGE";


}