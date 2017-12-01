
DROP TABLE IF EXISTS datatypes1;
create external table datatypes1 (
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
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334", "monarch.redundancy"="1");