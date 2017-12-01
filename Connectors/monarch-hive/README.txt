---
title: Monarch-Hive Client Integration
---

## Introduction

Ampool has developed a new storage handler for Hive that stores data into an Ampool Table rather than HDFS.Ampool, a highly concurrent in-memory store, is able speed up response time for many queries, complete queries that may otherwise fail due to lack of resources. It also allows for greater concurrency and data access by multiple jobs is less sensitive to I/O bandwidth issues.

## How to Use

Un-tar the package and you should have the following:

```
monarch-hive-<version>
├── dependencies
│   ├── monarch-client-dependencies-<version>.jar
│   ├── monarch-<version>.jar
│   ├── fastutil-<version>.jar
│   ├── javax.transaction-api-<version>.jar
│   ├── log4j-api-<version>.jar
│   └── log4j-core-<version>.jar
│   └── shiro-core-<version>.jar
├── lib
│   └── monarch-hive-<version>.jar
└── README.txt
```

The *lib* directory contains the jar file for this connector and *dependencies* directory contains all dependent libraries.

For using Monarch-Hive integration, you will need a running Monarch cluster. The details for setting up an Monarch cluster can be found at http://docs.ampool-inc.com/w_MASH/. Once the cluster is setup, the locator host-name and port will be required further.

## Monarch Properties
### Table Creation Properties
Following set of properties can be used when using Monarch storage handler for Hive tables. These properties can be specified via ```tblproperties``` when creating the tables from Hive CLI. Some properties (like monarch.locator.host) are client level properties and these are used only once and the same client is used for all further interactions. Such properties are ignored, even if specified, for all subsequent commands.

**NOTE:** By default, creates a table of type Immutable (FTable).

| Property | Details | Default Value |
|----------|---------|---------------|
| monarch.locator.host | The locator host-name to connect.<br/> _This is a client level property and utilized only for the first time when creating a client connection._ | localhost |
| monarch.locator.port | The locator port to connect.<br/> _This is a client level property and utilized only for the first time when creating a client connection._ | 10334 |
| monarch.read.timeout | The client-read-timeout in milliseconds.<br/> _This is a client level property and utilized only for the first time when creating a client connection._ | 10000 |
| monarch.redundancy | The number of redundant copies of the data | 0 |
| monarch.buckets | The number of buckets/partitions | 113 |
| monarch.table.type | The table type to be used for reading/writing from Ampool.<br/> The value should be either of: **immutable**, **unordered**, **ordered**. | immutable |
| monarch.enable.persistence | The mode of writing data to persistent storage.<br/> The value should be either: **async** or **sync**.<br/> _Applicable for **unordered** and **ordered** tables._ | None |
| monarch.partitioning.column | The partitioning column (hash-code of the column-value) to be used for data distribution.<br/> _Applicable only for **immutable** tables._ | None |
| monarch.max.versions | The maximum number of versions to be stored in a table. <br/> _Applicable only for **ordered** tables._ | None |
| monarch.write.key.column | The index of the column that should be used as key when writing data into mutable Ampool tables. <br/> _Applicable for **unordered** and **ordered** tables._ | None |

### Query Properties
Apart from the above properties (available at table creation time), following properties can be used from Hive CLI when querying the data from Ampool tables.

| Property | Details | Default Value |
|----------|---------|---------------|
| monarch.read.max.versions | The number of maximum versions to be retrieved during the query. | 1 |
| monarch.read.filter.latest | Use only latest version for executing filters, if any, during the query. | true |

For example, you can set a property from Beeline before executing a query:
```
set monarch.read.max.versions=10;
select count(*) from monarch_table;
```
Above query will retrieve maximum upto 10 versions of each row, if present, from ```monarch_table```.

_Note: The query properties are applicable only for the tables that have more than one version. At the moment, these will be applicable only for **ordered** tables._

## The Column Types
Monarch MTable supports all Hive types. The column types from a Hive are mapped to the respective MTable column types. All the basic types with following complex types are supported:

* Array
* Struct
* Map
* Union

## Using Beeline or JDBC API with Hive Server2

(HIVE_HOME is hive installation directory); eg: if this is an HDP installation: /usr/hdp/<version>/hive

1. On the machine running Hive server 2, run the following commands:

```
$ sudo mkdir $HIVE_HOME/auxlib
$ sudo cp monarch-hive-<version>/lib/*.jar to  $HIVE_HOME/auxlib
$ sudo cp monarch-hive-<version>/dependencies/*.jar to  $HIVE_HOME/auxlib
```

2. Restart hive server 2.

## Running Hive Queries (using Beeline)

To run Hive queries (create/insert/select) using Beeline you need:
1. A running Monarch cluster
2. The required libraries are copied to _$HIVE_HOME/auxlib_ and Hive server is restarted

### Create Table

Creating a Hive table with external storage handler, stores only the table meta-data in Hive whereas the actual table data is stored by the respective storage handler (i.e. Monarch in this case).

Following command can be used from Beeline to create a table:

```
CREATE TABLE monarch_table_1(id INT, time STRING, count BIGINT)
   STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
   tblproperties ("monarch.locator.host"="localhost", "monarch.locator.port"="10334");
```

The command above creates a table called `monarch_table_1` with Monarch storage handler. Eventually, when the data is inserted, it is be stored in the respective Monarch cluster. By default, the table will be created with as **<database-name>_<table-name>** using the respective details from Hive.

In case you want to name the Monarch table differently, you can provide another property `monarch.region.name` and that name will be used for Monarch table. For example:

```
CREATE TABLE monarch_table_1(id INT, time STRING, count BIGINT)
   STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
   tblproperties ("monarch.locator.host"="localhost", "monarch.locator.port"="10334", "monarch.region.name"="my_ampool_table");
```

### Insert Data

You can use following command (or any other equivalent command) to insert data into a table backed by Monarch:

```
INSERT OVERWRITE TABLE monarch_table_1 SELECT my_id, my_time, my_count FROM <existing-hive-table>;
```

### Query Data

Standard Hive queries can be used as it is to retrieve the data in Hive tables backed by Monarch. For example:

```
SELECT * FROM monarch_table_1;
SELECT * FROM monarch_table_2;
```

### Load Existing Monarch table as External table

To create external Hive table which references existing (pre-created) MTable as underlying storage as:

```
CREATE EXTERNAL TABLE monarch_external_table(id INT, time STRING, count BIGINT)
   STORED BY "io.ampool.monarch.hive.MonarchStorageHandler"
   tblproperties ("monarch.locator.host"="host", "monarch.locator.port"="port", "monarch.region.name"="my_ampool_table");
```

**Note**:
    Existing MTable column types must match the respective column types of the external hive table.

### Drop Table

The Hive tables backed by Monarch can be deleted as normal Hive tables. When you drop a Hive table, respective Monarch table is also deleted, except for external tables. When external Hive table is dropped, only the Hive table definition is dropped, keeping Monarch table untouched.

To drop a table, you can use the following command:

```
DROP TABLE monarch_external_table;
DROP TABLE monarch_table_1;
```