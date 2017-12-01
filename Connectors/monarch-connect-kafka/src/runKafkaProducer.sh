#!/usr/bin/env bash
# Before running this script, create the topics with their configuration like,
#./bin/kafka-topics --create --zookeeper nil-instance-1.c.ampool-141120.internal:2181 --replication-factor 1  --partitions 43 --topic topic1
#./bin/kafka-topics --create --zookeeper nil-instance-1.c.ampool-141120.internal:2181 --replication-factor 1  --partitions 79 --topic topic2
#./bin/kafka-topics --create --zookeeper nil-instance-1.c.ampool-141120.internal:2181 --replication-factor 1  --partitions 113 --topic topic3
# script taks three parameters.
#1. topic name
#2. number of records in each partition.
#3. number of partitions created for the topic.

if [ -n "$CONFLUENT_HOME" ]
then
  echo "CONFLUENT_HOME is set to $CONFLUENT_HOME "
else
  CONFLUENT_HOME=/home/ampool/confluent-oss-3.2.0-2.11
fi
TOPIC=$1
NUMBER=$2
PARTITIONS=$(( $3 - 1 ))
   echo "Topic = $TOPIC and Number = $NUMBER "
for i in $(seq 0 $PARTITIONS)
do
   echo "Welcome $i times"
   java -cp "$CONFLUENT_HOME/share/java/kafka/*:$CONFLUENT_HOME/share/java/kafka-serde-tools/*:$CONFLUENT_HOME/share/java/confluent-common/*:$CONFLUENT_HOME/share/java/schema-registry/*":. SimpleProducer $TOPIC $NUMBER $i

done
