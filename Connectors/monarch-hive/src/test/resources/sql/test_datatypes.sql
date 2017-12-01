USE ${hiveconf:my.schema};

DROP TABLE IF EXISTS datatypes;
create table datatypes (
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

load data local inpath '${hiveconf:MY.HDFS.DIR}/ampool/datatypes.data' into table datatypes;

DROP TABLE IF EXISTS datatypes1;
create table datatypes1 (
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
  TBLPROPERTIES ("monarch.region.name"="datatypes0",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334",
   "monarch.read.timeout"="12345", "monarch.block.size"="1", "monarch.block.format"="AMP_BYTES");

insert overwrite table datatypes1 select col_tinyint,
col_smallint, col_int, col_bigint, col_boolean,
col_float, col_double, col_string, col_int_array,
col_string_array, col_map, col_struct, col_timestamp, col_binary,
 col_decimal, col_char, col_varchar, col_date from datatypes;

DROP TABLE IF EXISTS datatypes2;
create table datatypes2 (
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
  TBLPROPERTIES ("monarch.region.name"="datatypes2",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334",
  "monarch.partitioning.column"="col_tinyint", "monarch.redundancy"="3", "monarch.buckets"="11",
  "monarch.block.size"="1", "monarch.block.format"="AMP_BYTES");

insert overwrite table datatypes2 select col_tinyint,
col_smallint, col_int, col_bigint, col_boolean,
col_float, col_double, col_string, col_int_array,
col_string_array, col_map, col_struct, col_timestamp,
col_decimal, col_char, col_varchar, col_date from datatypes;
