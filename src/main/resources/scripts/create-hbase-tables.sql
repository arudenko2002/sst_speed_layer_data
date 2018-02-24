

create 'artists_v4', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'test_project', 'units', 'euro', 'stream_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'test_track', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units'


create 'artist_rtd_test', 'units', 'euro', 'stream_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'top_track', 'track', 'units', 'euro'

create 'top_tracks', 'track', 'units', 'euro'

create 'top_artist_perftest_hist_v1', 'artist', 'units', 'euro', 'album_adjusted_units'

create 'top_track_perftest_hist_v1', 'track', 'units', 'euro', 'track_adjusted_units'



create 'top_project_perftest_hist_v1', 'project', 'units', 'euro', 'album_adjusted_units'

create 'test_top_artist', 'artist', 'units', 'euro', 'album_adjusted_units'

create 'test_top_project', 'project', 'units', 'euro', 'album_adjusted_units'

create 'test_top_track', 'track', 'units', 'euro', 'track_adjusted_units'

create 'isrc_v2', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units'

create 'project_perftest_hist_v1', 'units', 'euro', 'album_adjusted_units', 'stream_units', 'stream_album_adjusted_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'album_v1', 'units', 'euro', 'album_adjusted_units'

create 'test_isrc', 'units', 'euro', 'track_adjusted_units'

create 'test_album', 'units', 'euro', 'album_adjusted_units'

create 'track_perftest_hist_v1', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units'


#split by testing statistics
create 'test_artists_may9th_nosplitsplit_80', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'
create 'test_artists_may9th_split_uniform_80BT_20', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', {NUMREGIONS => 20, SPLITALGO => 'UniformSplit'}
create 'test_artists_may9th_split_uniform_80BT_50', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', {NUMREGIONS => 50, SPLITALGO => 'UniformSplit'}
create 'test_artists_may9th_split_uniform_80BT_80', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', {NUMREGIONS => 80, SPLITALGO => 'UniformSplit'}
create 'test_artists_may9th_split_hex_80BT_80', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', {NUMREGIONS => 80, SPLITALGO => 'HexStringSplit'}
create 'test_artists_may10th_split_predef_80BT', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', SPLITS => ['1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', '1019', '1020', '1021', '1022', '1023', '1024', '1025', '1026', '1027', '1028']
create 'test_artists_may10th_split_predef_80BT_56SPlit', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', SPLITS => ['10000', '10005', '10010', '10015', '10020', '10025', '10030', '10035', '10040', '10045', '10050', '10055', '10060', '10065', '10070', '10075', '10080', '10085','10090', '10095', '10100', '10105', '10110', '10115', '10120', '10125', '10130',  '10135', '10140', '10145', '10150', '10155', '10160', '10165', '10170', '10175', '10180', '10185', '10190', '10195', '10200', '10205', '10210', '10215','10220', '10225', '10230', '10235', '10240', '10245', '10250', '10255', '10260', '10265', '10270', '10275', '10280', '10285', '10290']
scan 'hbase:meta',{FILTER=>"PrefixFilter('test_artists_may9th_nosplitsplit_80')"}

#uat table version
create 'artist_uat_v1', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'track_uat_v1', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units'

create 'project_uat_v1', 'units', 'euro', 'album_adjusted_units', 'stream_units', 'stream_album_adjusted_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro'

create 'top_artist_uat_v1', 'artist', 'units', 'euro', 'album_adjusted_units'

create 'top_track_uat_v1', 'track', 'units', 'euro', 'track_adjusted_units'

create 'top_track_uat_v2', 'track', 'units', 'euro', 'track_adjusted_units'

create 'top_project_uat_v1', 'project', 'units', 'euro', 'album_adjusted_units'

create 'top_project_uat_v2', 'project', 'units', 'euro', 'album_adjusted_units'

create 'album_uat_v1', 'units', 'euro', 'album_adjusted_units'

create 'isrc_uat_v1', 'units', 'euro', 'track_adjusted_units'


#uat incremental table version
create 'artist_inc_uat_v1', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', 'digital_album_album_adjusted_units'

create 'track_inc_uat_v1', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units', 'digital_track_track_adjusted_units', 'album_adjusted_units'

create 'project_inc_uat_v1', 'units', 'euro', 'album_adjusted_units', 'stream_units', 'stream_album_adjusted_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', 'digital_album_album_adjusted_units'

create 'top_artist_inc_uat_v1', 'artist', 'units', 'euro', 'album_adjusted_units'

create 'top_track_inc_uat_v1', 'track', 'units', 'euro', 'track_adjusted_units'

create 'top_project_inc_uat_v1', 'project', 'units', 'euro', 'album_adjusted_units'

create 'album_inc_uat_v1', 'units', 'euro', 'album_adjusted_units', 'preorder_units'

create 'isrc_inc_uat_v1', 'units', 'euro', 'track_adjusted_units'



#uat 2  table version
create 'artist_uat2_v1', 'units', 'euro', 'album_adjusted_units', 'audio_stream_units',  'video_stream_units', 'audio_stream_album_adjusted_units',  'video_stream_album_adjusted_units', 'audio_stream_euro',  'video_stream_euro',  'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', 'digital_album_album_adjusted_units'

create 'track_uat2_v1', 'units', 'euro', 'audio_stream_units', 'audio_stream_euro', 'video_stream_units', 'video_stream_euro', 'digital_track_units', 'digital_track_euro', 'track_adjusted_units', 'audio_stream_track_adjusted_units', 'video_stream_track_adjusted_units', 'digital_track_track_adjusted_units', 'album_adjusted_units'

create 'project_uat2_v1', 'units', 'euro', 'album_adjusted_units', 'stream_units', 'stream_album_adjusted_units', 'stream_euro', 'physical_album_units', 'digital_album_units', 'digital_track_units', 'digital_track_album_adjusted_units', 'physical_album_euro', 'digital_album_euro', 'digital_track_euro', 'digital_album_album_adjusted_units'

create 'top_artist_uat2_v1', 'artist', 'units', 'euro', 'album_adjusted_units'

create 'top_track_uat2_v1', 'track', 'units', 'euro', 'track_adjusted_units'

create 'top_project_uat2_v1', 'project', 'units', 'euro', 'album_adjusted_units'

create 'album_uat2_v1', 'units', 'euro', 'album_adjusted_units', 'preorder_units'

create 'isrc_uat2_v1', 'units', 'euro', 'track_adjusted_units'