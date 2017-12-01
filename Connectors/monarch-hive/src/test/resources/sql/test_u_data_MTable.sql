USE ${hiveconf:my.schema};

DROP TABLE IF EXISTS u_data;
CREATE TABLE u_data (
  userid INT,
  movieid INT,
  rating INT,
  unixtime STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:MY.HDFS.DIR}/ampool/u.data' OVERWRITE INTO TABLE u_data;

DROP TABLE IF EXISTS m1;
CREATE TABLE m1(id int, name string, age int)
  STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.region.name"="m0",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334", "monarch.table.type"="__type__");

INSERT OVERWRITE TABLE m1 SELECT userid,unixtime,movieid FROM u_data;

DROP TABLE IF EXISTS m2;
CREATE TABLE m2(id int, name string, age int)
  STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
   TBLPROPERTIES ("monarch.locator.port"="10334", "monarch.redundancy"="2", "monarch.buckets"="57", "monarch.table.type"="__type__"
   , "monarch.enable.persistence"="async");

INSERT OVERWRITE TABLE m2 SELECT userid,unixtime,movieid FROM u_data;
