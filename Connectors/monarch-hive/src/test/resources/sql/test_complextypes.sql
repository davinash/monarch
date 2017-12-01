USE ${hiveconf:my.schema};

DROP TABLE IF EXISTS complextypes;
create table complextypes (
simple_int int,
max_nested_array  array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_map    array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<map<string,string>>>>>>>>>>>>>>>>>>>>>>,
max_nested_struct array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<struct<s:string, i:bigint>>>>>>>>>>>>>>>>>>>>>>>,
simple_string string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
   'hive.serialization.extend.nesting.levels'='true',
   'line.delim'='\n'
)
;

load data local inpath '${hiveconf:MY.HDFS.DIR}/ampool/datatypes.data' overwrite into table complextypes;

DROP TABLE IF EXISTS complextypes1;
create table complextypes1 (
simple_int int,
max_nested_array  array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_map    array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<map<string,string>>>>>>>>>>>>>>>>>>>>>>,
max_nested_struct array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<struct<s:string, i:bigint>>>>>>>>>>>>>>>>>>>>>>>,
simple_string string
) STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES (
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334");

insert overwrite table complextypes1 select simple_int, max_nested_array, max_nested_map, max_nested_struct, simple_string from complextypes;
