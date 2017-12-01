USE ${hiveconf:my.schema};

DROP TABLE IF EXISTS mdatatypes;
create table mdatatypes (
col_tinyint tinyint,
col_smallint smallint,
col_int int,
col_bigint bigint,
col_boolean boolean,
col_float float,
col_double double,
col_string string,
col_int_array array<int>,
col_string_array array<string>,
col_map map<int,string>,
col_struct struct< id:string, name:string, val:int>,
col_timestamp timestamp,
col_binary binary,
col_decimal decimal(10, 5),
col_char char(10),
col_varchar varchar(26),
col_date date
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' STORED AS TEXTFILE;

load data local inpath '${hiveconf:MY.HDFS.DIR}/ampool/datatypes.data' into table mdatatypes;

DROP TABLE IF EXISTS mdatatypes1;
create table mdatatypes1 (
col_tinyint tinyint,
col_smallint smallint,
col_int int,
col_bigint bigint,
col_boolean boolean,
col_float float,
col_double double,
col_string string,
col_int_array array<int>,
col_string_array array<string>,
col_map map<int,string>,
col_struct struct< id:string, name:string, val:int>,
col_timestamp timestamp,
col_binary binary,
col_decimal decimal(10, 5),
col_char char(10),
col_varchar varchar(26),
col_date date
) STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.region.name"="mdatatypes0",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334",
  "monarch.table.type"="unordered", "monarch.block.size"="1", "monarch.block.format"="AMP_BYTES");

insert overwrite table mdatatypes1 select col_tinyint,
col_smallint, col_int, col_bigint, col_boolean,
col_float, col_double, col_string, col_int_array,
col_string_array, col_map, col_struct, col_timestamp, col_binary,
 col_decimal, col_char, col_varchar, col_date from Mdatatypes;

DROP TABLE IF EXISTS mdatatypes2;
create table mdatatypes2 (
col_tinyint tinyint,
col_smallint smallint,
col_int int,
col_bigint bigint,
col_boolean boolean,
col_float float,
col_double double,
col_string string,
col_int_array array<int>,
col_string_array array<string>,
col_map map<int,string>,
col_struct struct< id:string, name:string, val:int>,
col_timestamp timestamp,
--col_binary binary,
col_decimal decimal(10, 5),
col_char char(10),
col_varchar varchar(26),
col_date date
) STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.region.name"="mdatatypes2",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334",
  "monarch.table.type"="unordered", "monarch.redundancy"="3", "monarch.buckets"="11",
  "monarch.block.size"="1", "monarch.block.format"="AMP_BYTES");

insert overwrite table mdatatypes2 select col_tinyint,
col_smallint, col_int, col_bigint, col_boolean,
col_float, col_double, col_string, col_int_array,
col_string_array, col_map, col_struct, col_timestamp,
col_decimal, col_char, col_varchar, col_date from mdatatypes;
