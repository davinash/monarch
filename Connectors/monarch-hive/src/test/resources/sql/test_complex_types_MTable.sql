
DROP TABLE IF EXISTS Mdummy;

CREATE TABLE Mdummy(id int);

INSERT INTO TABLE Mdummy VALUES(1);

DROP TABLE IF EXISTS Mcomplex_types;

CREATE TABLE Mcomplex_types (
c1 int,
c21 array<array<int>>,
c22 array<decimal(10,5)>,
c23 map<string,decimal(10,5)>,
c24 struct<c1:boolean,c2:decimal(10,5)>,
c25 array<char(5)>,
c3 array<map<string,struct<c1:boolean,c2:decimal(10,5),c3:varchar(10)>>>,
c4 array<struct<c1:string,c3:double>>,
c5 array<struct<c1:string,c2:map<int,string>,c3:double>>,
c6 map<int, array<string>>,
c7 map<string, struct<c1:int,c2:string>>,
c8 map<string,array<struct<c1:string,c2:map<int,string>,c3:double>>>,
c9 struct<c1:map<string,array<int>>,c2:array<map<int,double>>,c3:struct<c11:int,c22:string>>,
cl int
) STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
  TBLPROPERTIES ("monarch.locator.port"="10334", "monarch.table.type"="unordered", "monarch.enable.persistence"="async");


INSERT INTO TABLE Mcomplex_types SELECT
 1,
 array(array(1,2,3),array(4,5,6),array(7,8,9)),
 array(cast(0.12345678 as decimal(10,5)), cast(1.2345678 as decimal(10,5)), cast(12.345678 as decimal(10,5))),
 map('string_1', cast(0.12345678 as decimal(10,5))),
 named_struct('c1', true, 'c2', cast(12.34568 as decimal(10,5))),
 array(cast('abc' as char(5)), cast('def' as char(5))),
 array(map('string_1', named_struct('c1', true, 'c2', cast(1.234567 as decimal(10,5)), 'c3', cast('abc' as varchar(10)))),
  map('string_2',  named_struct('c1', false, 'c2', cast(12.34567 as decimal(10,5)), 'c3', cast('def' as varchar(10)))),
  map('string_3', named_struct('c1', true, 'c2', cast(123.4567 as decimal(10,5)), 'c3', cast('ghi' as varchar(10))))),
 array(named_struct('c1', 'string_1', 'c3', 11.11), named_struct('c1', 'string_3', 'c3', 22.22), named_struct('c1', 'string_2', 'c3', 33.33)),
 array(named_struct('c1', 'string_11', 'c2', map(11, 'value_11', 12, 'value_12', 13, 'value_13'), 'c3', 123.456),
  named_struct('c1', 'string_22', 'c2', map(21, 'value_21', 22, 'value_22', 23, 'value_23'), 'c3', 123.456)),
 map(1, array('string_1', 'string_2', 'string_3')),
 map('key_1', named_struct('c1', 11, 'c2', 'string_11'), 'key_2', named_struct('c1', 12, 'c2', 'string_12')),
 map('s_1', array(named_struct('c1', 'string_11', 'c2', map(11, 'value_11', 12, 'value_12', 13, 'value_13'), 'c3', 123.456))),
 named_struct('c1', map('key_11', array(1,2,3), 'key_12', array(4,5,6)),
   'c2', array(map(11, 11.111, 12, 11.222, 13, 11.333),
               map(21, 22.111, 22, 22.222, 23, 22.333),
               map(31, 33.111, 32, 33.222, 33, 33.333)),
    'c3', named_struct('c11', 111, 'c22', 'string_222')),
 3 from Mdummy;