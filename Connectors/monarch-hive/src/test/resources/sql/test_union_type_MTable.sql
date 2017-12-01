
DROP TABLE IF EXISTS Mdummy;

CREATE TABLE Mdummy(id int);

INSERT INTO TABLE Mdummy VALUES(1);

DROP TABLE IF EXISTS Mtest_union_type;

CREATE TABLE Mtest_union_type (
c1 int,
c2 uniontype<int,string>,
c3 uniontype<int,string>,
c4 uniontype<int,string,array<struct<c11:int,c12:map<string,double>,c13:double>>>,
cl struct<c1:int>
) STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
 TBLPROPERTIES ("monarch.locator.port"="10334", "monarch.table.type"="unordered"
 , "monarch.enable.persistence"="async");

INSERT INTO TABLE Mtest_union_type SELECT
 1,
 create_union(0, 123, 'string_111'),
 create_union(1, 123, 'string_111'),
 create_union(2, 123, 'string_111', array(named_struct('c11', 11, 'c12', map('key_11', 11.111,
 'key_12', 22.111, 'key_13', 33.111), 'c13', 123.456),
  named_struct('c11', 22, 'c12', map('key_21', 22.111, 'key_22', 22.222, 'key_23', 33.222),
  'c13', 456.321))),
 named_struct('c1', 1234)
 from Mdummy;