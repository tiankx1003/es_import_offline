create table if not exists tmp.es_import_offline(
  pri_key string comment 'primary_key',
  value01 string comment 'value01',
  value02 string comment 'value02'
) comment 'mock source datum'
partitioned by (dt string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile
;

load data local inpath '/opt/files/tmp.es_import_offline.tsv' into table tmp.es_import_offline partition(dt = '20991231');


create table if not exists tmp.abd_index_list(
  name string comment 'table_name',
  theme string comment 'index_theme'
) comment 'abandon index list'
partitioned by (dt string);


insert into table tmp.abd_index_list partition(dt = '20991231')
(name, theme) values
('es_import_offline_abd', 'test');


create table if not exists tmp.column_type_index_mapping(
  index_name string comment '',
  data_type string comment ''
) comment 'hive to es column type mapping'
partitioned by (dt string);

insert into table tmp.column_type_index_mapping partition(dt = '20991231')
(index_name, data_type) values
('pri_key', 'string'),
('value01', 'string'),
('value02', 'string')
;





