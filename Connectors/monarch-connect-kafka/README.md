# Monarch sink connector for kafka.

## monarch-connect-kafka.
monarch-connect-kafka is a Kafka Sink-Connector for storing data from kafka (topics) to corresponding monarch tables.

## How to Use

Untar the package **monarch-connect-kafka-<connector_version>.tar.gz** and you should have the following:

```
monarch-connect-kafka-<connector_version>
├── dependencies
│   ├── monarch-client-dependencies-<version>.jar
│   ├── monarch-<version>.jar
│   ├── fastutil-<version>.jar
│   ├── javax.transaction-api-<version>.jar
│   ├── log4j-api-<version>.jar
│   ├── log4j-core-<version>.jar 
│   └── shiro-core-<version>.jar
├── lib
│   └── monarch-connect-kafka-<connector_version>.jar
├── README.md
└── AmpoolEULA.pdf
```
The *lib* directory contains the jar file for this connector and *dependencies* directory contains all dependent libraries

## Pre-requisites.
* confluent-oss [_version 3.2.0_2.11 or latest_]
* Monarch 1.0.0-SNAPSHOT or latest
* JDK 1.8

__NOTE: Make sure that monarch-connect-kafka-<version> is compatible with the version of the Monarch server cluster you are using.__

## Assumptions
* Each kafka topic is mapped to an Monarch table that is expected to be pre-existing with the desired configuration (#buckets, #replicas etc).
* No mapping between Kafka partitioning and Monarch table partitioning.

## Schema Type (kafka) to Monarch type mapping

* **BOOLEAN** ---	**MBasicObjectType.BOOLEAN**
* **FLOAT32** ---	**MBasicObjectType.FLOAT**
* **FLOAT64** ---	**MBasicObjectType.DOUBLE**
* **INT8**	  ---  **MBasicObjectType.BYTE**
* **INT16**	  ---  **MBasicObjectType.SHORT**
* **INT32**	  ---  **MBasicObjectType.INT**
* **INT64**	  ---  **MBasicObjectType.LONG**
* **STRING**  ---  **MBasicObjectType.STRING**
* **BYTES**   ---  **MBasicObjectType.BINARY**
* **Date**	  ---  **BasicTypes.Date**
* **Decimal or BigDecimal**	  ---  **BasicTypes.BigDecimal**
* **Time**  ---  **BasicTypes.Timestamp**
* **Timestamp**   ---  **BasicTypes.Timestamp**
In current implementation, types specified in the table above are only supported.

## Sink connector properties

| Name | Description | Default |
|------------|-------------| -------------|
| name    | Unique name for the connector. Attempting to register again with the same name will fail. |  |
| connector.class    | The Java class for the sink connector. io.ampool.monarch.kafka.connect.AmpoolSinkConnector   | |
| tasks.max    | The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism. |  |
| locator.host       | The host name or IP address to which the Monarch locator is bound. |  |
| locator.port      | Monarch locator service port |  |
| batch.size      | Number of records to be inserted in single put/append operation while writing records from topic to ampool tables. Default value of 1000 is recommended (For the usecase having total 10 columns each having a regular size). User may adjust it according to number of columns and value of each column.  | 1000 |
| ampool.tables      | Comma seperated list that represents a monarch table names. Example: table1,table2,table3. Total number of table names has to match with total number of topic names specified by **topics**. Table needs to exist, so user has to create all table manually in ampool before ingesting data into topics.  |  |
| topics     |  Comma seperated list that represents a kafka topic names. Example: topic1,topic2,topic3. Tables and topics are mapped according to their respective order in a comma seperated list, so with the example given, topic to table mapping is [topic1 -> table1, topic2 -> table2 and topic3 -> table3] |  |
| ampool.tables.rowkey.column      | Rowkey to be used for a MTable only. Useful to map a primary-key as a rowkey. Needs to specify in a TOPICNAME:ROWKEY format. If not specified, unique rowkey is generated as a combination of  **topic partiton offset**, each element separated by pipe characher. | none |


## Sample Sink connector properties (sink-connector.properties)
```
name=monarch-sink-connector
connector.class=io.ampool.monarch.kafka.connect.AmpoolSinkConnector
tasks.max=1
locator.host=localhost
locator.port=10334
batch.size=10
ampool.tables=test
topics=test
ampool.tables.rowkey.column=test:id
```
**NOTE:**One can use a property **ampool.tables.rowkey.column** to explicitly set a row-key in a corresponding MTable. Value for this property has to be a valid column name.
 In the above sample configuration value of column **id** will be used as a row-key when stored in a Monarch ADS. This property is only applicable to MTable.

## Running in development (Standalone mode)
1. Setup a single-node(localhost) monarch cluster containing one locator and one server and create a table.
```
mash>create table --type=UNORDERED --name=test --schema-json="{"id":"INT","name":"STRING"}"
```
2. Download and install confluent-oss-3.2.0-2.11. export confluent base/home dir as CONFLUENT_HOME.

3. Extract monarch-connect-kafka package,
   e.g: tar -xvzf monarch-connect-kafka-<version>.tar.gz
   Let's call the base dir monarch-connect-kafka-<version> as MONARCH_CONNECT_KAFKA_HOME
   create a sample configuration file in dir MONARCH_CONNECT_KAFKA_HOME as shown above and name it as monarch-sink.properties.
   Create $CONFLUENT_HOME/share/java/monarch-connect-kafka dir and Copy the following connector files as shown bellow.

```
cd $CONFLUENT_HOME
mkdir -p $CONFLUENT_HOME/share/java/monarch-connect-kafka
cp $MONARCH_CONNECT_KAFKA_HOME/lib/monarch-connect-kafka-<version>.jar $CONFLUENT_HOME/share/java/monarch-connect-kafka
mkdir -p $CONFLUENT_HOME/etc/monarch-connect-kafka
cp $MONARCH_CONNECT_KAFKA_HOME/monarch-sink.properties $CONFLUENT_HOME/etc/monarch-connect-kafka/
```

4. Start Zookeeper, Kafka and Schema registry

```
./bin/zookeeper-server-start etc/kafka/zookeeper.properties
./bin/kafka-server-start etc/kafka/server.properties
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
```

5. Start monarch-sink-connector (This will get launched with a worker node). Set the CLASSPATH to add required dependencies.
```
export CLASSPATH="$CONFLUENT_HOME/share/java/monarch-connect-kafka/monarch-connect-kafka-<version>.jar:$MONARCH_CONNECT_KAFKA_HOME/dependencies/monarch-client-dependencies-<version>.jar:$MONARCH_CONNECT_KAFKA_HOME/dependencies/monarch-<version>.jar"
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/monarch-connect-kafka/monarch-sink.properties
```

6. Test with avro console, start the console to create the topic and write values
```
./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test --property value.schema='{"type":"record","name":"record","fields":[{"name":"id","type":"int"}, {"name":"name", "type": "string"}]}'

#ingest records in a kafka topic
{"id": 1, "name": "myName1"}
{"id": 2, "name": "myName2"}
{"id": 3, "name": "myName3"}
{"id": 4, "name": "myName4"}
{"id": 5, "name": "myName5"}
```
7. Verify that above records are getting inserted into the corresponding Ampool table.

**Note:** For the above example, table rowKey is a "topic|partitionId|offset" and rowValue contains columns: id, name with their respective values.
