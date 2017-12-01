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

DROP TABLE IF EXISTS p1;
CREATE TABLE p1(id int, name string, age int)
  STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.region.name"="p0",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334");

INSERT OVERWRITE TABLE p1 SELECT userid,unixtime,movieid FROM u_data;

DROP TABLE IF EXISTS p2;
CREATE EXTERNAL TABLE p2(id int, name string, age int)
  STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.region.name"="p0",
  "monarch.locator.host"="localhost", "monarch.locator.port"="10334");

INSERT OVERWRITE TABLE p2 SELECT userid,unixtime,movieid FROM u_data;

DROP TABLE IF EXISTS p3;
CREATE TABLE p3(id int, name string, age int)
  STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
   TBLPROPERTIES ("monarch.locator.port"="10334", "monarch.redundancy"="2", "monarch.buckets"="57",
   "monarch.block.format"="AMP_BYTES");

INSERT OVERWRITE TABLE p3 SELECT userid,unixtime,movieid FROM u_data;
