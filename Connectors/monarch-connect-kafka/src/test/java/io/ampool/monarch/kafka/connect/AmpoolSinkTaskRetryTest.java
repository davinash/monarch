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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AmpoolSinkTaskRetryTest {

  private final Map<String, String> configProps = new HashMap<>();

  private static final String TABLE1 = "test1";
  private static final String TOPIC1 = "topic1";
  private static final int MAX_RETRIES = 5;


  @BeforeTest
  public void setUpBeforeMethod() throws Exception {

    //kafka sink-connector/task config.
    configProps.put("locator.port", "10334");
    configProps.put("locator.host", MonarchUtils.LOCATOR_HOST);
    configProps.put("batch.size", String.valueOf(1));
    configProps.put("ampool.tables", TABLE1);
    configProps.put("topics", TOPIC1);
    configProps.put("max.retries", Integer.toString(MAX_RETRIES));
    configProps.put("retry.interval.ms", Integer.toString(30000));
  }

  @AfterTest
  public void setUpAfterMethod() throws Exception {
    configProps.clear();
  }

  /**
   * Testcase - SinkTask throws RetriableException if ampool cluster is down.
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithCluserDown(){

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    //Note: start does not throw any exception, it logs warn if cluster is not up and running.
    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC1.trim(), 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    //verify the retry mechanism
    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    }catch (Exception re) {
      //re.printStackTrace();
      if(re instanceof RetriableException){
        exception = re;
      }
    }
    assertTrue(exception instanceof RetriableException);

    sinkTask.stop();
  }

  /**
   * Testcase - In case of cluster down, Every invocation/reply of SinkTask.put(SinkRecords) throws RetriableException
   * till the value of configured param (i.e a"max.retries"). Once the #max.retries are completed and still cluster is down
   * sinkTask throws a ConnectException.
   */
  @org.testng.annotations.Test
  public void testAmpoolSinkTaskWithMultipleRetry(){

    AmpoolSinkTask sinkTask = new AmpoolSinkTask();

    sinkTask.start(configProps);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("url", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("zipcode", Schema.INT32_SCHEMA)
        .field("status", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int noOfRecords = 10;
    for (int i = 1; i <= noOfRecords; i++) {
      final Struct record = new Struct(valueSchema)
          .put("url", "google.com")
          .put("id", i)
          .put("zipcode", 95050 + i)
          .put("status", 400 + i);
      SinkRecord sinkRecord = new SinkRecord(TOPIC1.trim(), 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }

    //Verify, for each invocation of sinkTask.put(), RetriableException is thrown, if cluster is down.
    for(int i =1; i<=MAX_RETRIES; i++) {
      Exception exception = null;
      try {
        sinkTask.put(sinkRecords);
      } catch (Exception re) {
        if (re instanceof RetriableException) {
          exception = re;
        }
      }
      assertTrue(exception instanceof RetriableException);
      assertEquals((MAX_RETRIES - i), sinkTask.getRemainingRetries());
      //System.out.println("TEST-1: TEST Retries = " + (MAX_RETRIES - i) + "  RemainingRetries = " + sinkTask.getRemainingRetries());
    }

    //Validate that after all retries, user gets a ConnectException if cluster is down.
    Exception exception = null;
    try {
      sinkTask.put(sinkRecords);
    } catch (Exception ce) {
      if (ce instanceof ConnectException) {
        exception = ce;
      }
    }
    assertTrue(exception instanceof ConnectException);

    sinkTask.stop();
  }
}
