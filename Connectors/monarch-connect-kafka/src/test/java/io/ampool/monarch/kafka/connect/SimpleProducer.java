/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package io.ampool.monarch.kafka.connect;

// Compilation Step : javac -cp "/home/ampool/workspace/confluent-3.0.0/share/java/confluent-serializers/*:/home/ampool/workspace/confluent-3.0.0/share/java/kafka-serde-tools/*" *.java

// Running Step : java -cp "/home/ampool/workspace/confluent-3.0.0/share/java/confluent-serializers/*:/home/ampool/workspace/confluent-3.0.0/share/java/kafka-serde-tools/*:/home/ampool/workspace/confluent-3.0.0/share/java/confluent-common/*:/home/ampool/workspace/confluent-3.0.0/share/java/schema-registry/*":. SimpleProducer topicName 100
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

//Create java class named “SimpleProducer”
public class SimpleProducer {

  public static void main(String[] args) throws Exception{

    // Check arguments length value
    if(args.length == 0){
      System.out.println("Enter topic name");
      return;
    }

    //Assign topicName to string variable
    String topicName = args[0].toString();
    int number = Integer.parseInt(args[1]);
    int partition = Integer.parseInt(args[2]);

    // create instance for properties to access producer configs
    Properties props = new Properties();

    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092");

    //Set acknowledgements for producer requests.
    props.put("acks", "all");

    //If the request fails, the producer can automatically retry,
    props.put("retries", 0);

    //Specify buffer size in config
    props.put("batch.size", 16384);

    //Reduce the no of requests less than 0
    props.put("linger.ms", 1);

    //The buffer.memory controls the total amount of memory available to the producer for buffering.
    props.put("buffer.memory", 33554432);

    props.put("key.serializer",
      "io.confluent.kafka.serializers.KafkaAvroSerializer");

    props.put("value.serializer",
      "io.confluent.kafka.serializers.KafkaAvroSerializer");

    props.put("schema.registry.url", "http://localhost:8081");

    Producer<Object, Object> producer = new KafkaProducer
      <Object, Object>(props);

    String userSchema = "{\"type\":\"record\"," +
      "\"name\":\"myrecord\"," +
      "\"fields\":[{\"name\":\"url\",\"type\":\"string\"}, {\"name\":\"id\",\"type\":\"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);

    for(int i=0; i< number; i++) {
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("url", "google.com");
      avroRecord.put("id", i);
      ProducerRecord<Object, Object> record = new ProducerRecord(topicName, partition,"key", avroRecord);
      producer.send(record);
    }

    System.out.println("Messages sent successfully");
    producer.close();
  }
}
