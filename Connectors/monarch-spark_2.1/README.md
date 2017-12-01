---
title: Spark-Monarch Integration
---

## Introduction

Ampool provides seamless integration between Monarch Tables and Spark DataFrames. A Spark DataFrame is a distributed collection of data organized into named columns that is equivalent to a table in RDBMS. Spark provides SQL like query functionality as DataFrame API (like filters and aggregations). Along with it, one can also register a DataFrame as temporary table and use SQL queries similar to any RDBMS.

Monarch Table(MTable/FTable) also allows one to organize data into named columns (with respective types) that are distributed across nodes in an underlying Monarch cluster. DataFrame's and Monarch Table's are equivalent and can be mapped by columns with their respective types. This page discusses Spark DataFrame and Monarch Table integration that allows one to save Spark DataFrames as an Monarch Table with respective column types.

## How to Use

Un-tar the package and you should have the following:

```
monarch-<monarch_version>-spark_2.1
├── dependencies
│   ├── monarch-client-dependencies-<version>.jar
│   ├── monarch-<version>.jar
│   ├── fastutil-<version>.jar
│   ├── javax.transaction-api-<version>.jar
│   ├── log4j-api-<version>.jar
│   ├── log4j-core-<version>.jar 
│   └── shiro-core-<version>.jar
├── examples
│   ├── pom.xml
│   ├── cars.csv
│   └── src
│       └── main
│           └── java
│               └── io
│                   └── ampool
│                       └── examples
│                           └── spark
│                               ├── SparkExampleDF.java
│                               ├── SparkExampleDFUsingCSV.java
│                               ├── SparkExampleML.java
│                               └── udf
│                     		    └── SampleEqualsUDF.java 	
├── lib
│   └── monarch-<ampool_version>-spark_2.1.jar
└── README.md
```

The *lib* directory contains the jar file for this connector and *dependencies* directory contains all dependent libraries.

For using Spark-Monarch integration, you will need running Monarch cluster. Once the cluster is setup, the locator host-name and port will be required further.

Monarch cluster details can be provided with via Map with following properties:
- ampool.locator.host (default localhost)
- ampool.locator.port (default 10334)

Once all the required libraries are available and cluster is running, you can launch Spark jobs (via spark-shell or spark-submit) as below:
```
$ <spark-home>/bin/spark-shell --jars lib/monarch-<connector_version>-spark_2.1.jar,dependencies/monarch-client-dependencies-<connector_version>.jar
```

The above command will spawn a spark shell, with required libraries loaded, that will allow you to use standard DataFrame APIs to save and retrieve DataFrame to/from Monarch Table.

__NOTE: Make sure that Monarch version used on the client machine is compatible with the version of the Monarch cluster you are using.__

## How to use Included Examples
The _examples_ directory contains a Maven project with more examples. These examples demonstrate the usage of Spark DataFrames and Spark ML with Monarch.

To compile these examples, run following command from _examples_ directory:
```
$ mvn clean package
```

Once the examples are compiled, you can run a specific example as below from examples directory (specify Monarch cluster details via arguments):
```
$ <spark-home>/bin/spark-submit --class io.ampool.examples.spark.SparkExampleDF \
    --jars ../lib/monarch-<connector_version>-spark_2.1.jar,../dependencies/monarch-client-dependencies-<connector_version>.jar \
    target/monarch-<connector_version>-spark_2.1-examples-jar-with-dependencies.jar localhost 10334

$ <spark-home>/bin/spark-submit --class io.ampool.examples.spark.SparkExampleML \
    --jars ../lib/monarch-<connector_version>-spark_2.1.jar,../dependencies/monarch-client-dependencies-<connector_version>.jar \
        target/monarch-<connector_version>-spark_2.1-examples-jar-with-dependencies.jar localhost 10334

$ <spark-home>/bin/spark-submit --class io.ampool.examples.spark.SparkExampleDFUsingCSV \
    --jars ../lib/monarch-<connector_version>-spark_2.1.jar,../dependencies/monarch-client-dependencies-<connector_version>.jar \
        target/monarch-<connector_version>-spark_2.1-examples-jar-with-dependencies.jar localhost 10334

```

When these examples are run, there are lot of log messages displayed on console. You can get rid of these by redirecting standard errors to null (i.e. append ` 2> /dev/null` to the command).

The provided examples cover following scenarios:

* Save an existing Spark DataFrame as Monarch Table (by using the DataFrame column mapping)
* Load an exsisting Monarch Table as a Spark DataFrame (by using the Monarch Table column mapping)
* Load an existing Monarch Table as DataFrame with following operations:
  * Filters
  * Aggregations
  * Column Selections
* Load an existing Monarch Table as DataFrame, register it as a temporary table and then use Spark-SQL to query data from Monarch

## The Column Types
Monarch Table supports most of the types supported by Spark DataFrames. The column types from a Spark DataFrame are mapped to the respective Monarch Table column types.

Following basic Spark types are supported by connector:
* Binary
* Boolean
* Byte
* Date
* Double
* Float
* Integer
* Long
* Short
* String
* Timestamp
* Array
* Struct
* Map

Along with the above types __Spark-ML Vector__, the user defined type, is also natively supported by the connector. Both the _Sparse_ and _Dense_ vectors can be natively read and saved to Monarch. You can save and retrieve a Spark DataFrames with Vectors using connector, like any other.

## Spark DataFrame Read/Write Options
When DataFrame in spark is either read or written to Monarch table, following options can be used,

| Name | Description | Default |
|------------|-------------| -------------|
| ampool.locator.host       | Hostname for Monarch Cluster locator | localhost|
| ampool.locator.port       | Port number for Monarch Cluster Locator | 10334|
| ampool.batch.size         | Number of records to be inserted in single put/append operation while writing DataFrame to Monarch Table |1000|
| ampool.log.file           | Absolute local file path for client log used by Spark Executors interacting with Monarch cluster||
| ampool.table.type         | Type of the underlying Monarch Table to be specified when new Table is created while writing the DataFrame, Values, *_ordered_* for ORDERED MTable, *_unordered_* for UNORDERED MTable, *_immutable*_ for FTable.|immutable|
| ampool.enable.persistence | When new Monarch Table is created while writing the DataFrame, this option specifies type of persistence. Values, *_sync_* or *_async_*. | Persistence is disabled|
| ampool.table.redundancy   | When new Monarch Table is created while writing the DataFrame, this option specifies redundancy factor for the table.|0|
| ampool.batch.prefix       | The specified prefix used with each row-key in a batch. This is for a use case where multiple data-frames are stored to same Monarch MTable in batches. Specifying unique prefix for each batch/data-frame is mandatory when multiple data frames are stored in the same Monarch table. Monarch uses this batch prefix to generate unique key for each record in the data frame.  In case the user does not provide unique prefix for multiple batches, there may be data-loss as some of the existing keys may get overwritten. This is relevant only for MTable. For FTable the row keys are internal and generated based on insertion timestamp||
| ampool.key.columns        | When new Monarch Table is created while writing the DataFrame, this option specifies comma separated list of columns to use as unique row key. If this option is specified then ampool.batch.prefix should NOT be used.||
| ampool.partitioning.column| If new Monarch FTable is created while writing the DataFrame, this option specifies which column to use for data partitioning. | entire row is used as partitioning key|
| ampool.key.separator      | In case ampool.key.columns has multiple column names, the specified separator is used to join the string representation of column values to form a row-key.||
| ampool.max.versions       | Set the Maximum number of versions (Valid only for ORDERED_VERSIONED tables) to be kept in memory for versioning. Also during read query, gets the all available versions for each row.|1|
| ampool.read.filter.latest | During read query, whether to execute filters on every version of row and only to latest version of row (Valid only for ORDERED_VERSIONED tables)|false|

## Save DataFrame to Monarch

To save an existing DataFrame as Monarch Table you can execute following command (from spark-shell):

```scala
scala> val myDataFrame: DataFrame = ...
scala> val options = Map(("ampool.locator.host","localhost"), ("ampool.locator.port","10334"))
scala> myDataFrame.write.format("io.ampool").options(options).save("ampool_table")
```

The above command will create an Monarch Table called `ampool_table` in an Monarch cluster (using _localhost_ and _10334_ as locator host and port respectively). The schema of the created table will be equivalent to the schema of `myDataFrame`.

For example if you have json data as a source for a dataframe you can turn it into a DataFrame backed by an Monarch Table and do Spark operations on it:

```scala
scala> val options = Map(("ampool.locator.host","localhost"), ("ampool.locator.port","10334"))
scala> val jsonDataFrame = sqlContext.read.json(path)
scala> jsonDataFrame.write.format("io.ampool").options(options).save("json_table")
```

## Load DataFrame from Monarch

If you already have an Monarch Table (created either by saving a DataFrame or generated by another tool/framework) it can be loaded as a DataFrame in Spark (from spark-shell):

```scala
scala> val options = Map(("ampool.locator.host","localhost"), ("ampool.locator.port","10334"))
scala> val ampoolDataFrame = sqlContext.read.format("io.ampool").options(options).load("ampool_table")
scala> ampoolDataFrame.show()
scala> ampoolDataFrame.filter("size > 4096").show()
```

The above command will create a spark data-frame from existing monarch Table `ampool_table`. Once the Monarch Table is loaded as data-frame we can execute any supported data-frame operation on it, which will eventually translate/retrieve data from Monarch as required.

## Using Spark-SQL over Monarch

You can also load an existing Monarch table as DataFrame, register it as temporary table and then use the Spark SQL to query data from Monarch (from spark-shell):

```scala
scala> val options = Map(("ampool.locator.host","localhost"), ("ampool.locator.port","10334"))
scala> sqlContext.read.format("io.ampool").options(options).load("ampool_table").registerTempTable("my_table")
scala> val results = sqlContext.sql("select * from my_table where size = 4096")
scala> results.show()
scala> println(results.count())
scala> results.foreach(println)
```
Once the Monarch Table is loaded as a table in Spark, any valid Spark SQL query can be used on it which will retrieve data from the Monarch Table as required.

_Note that although the above examples are in scala, python shell can also be used._

## Using Custom Spark UDF

You can use the custom Spark UDFs along with Monarch connector. The sample custom UDF (```SampleEqualsUDF```) is provided in the _examples_. You need to register the UDF with the SQL Context with appropriate return type. Once registered, it can be used similar to any other built-in UDFs in SQL queries as below:
```scala
scala> val df = sqlContext.read.format("io.ampool").load("ampool_table")df.registerTempTable("my_table")
scala> sqlContext.udf.register("MyEquals", SampleEqualsUDF.SAMPLE_EQUALS_UDF, DataTypes.BooleanType)
scala> sqlContext.sql("select * from my_table where MyEquals(column_1, 'constant_value')").show()
```
