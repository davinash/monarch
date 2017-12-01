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
package io.ampool.monarch.hive;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * The Monarch input format handler for Hive.
 * <p>
 */
public class MonarchInputFormat implements InputFormat<Writable, Writable> {
  private JobConf conf;

  public MonarchInputFormat() {
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return MonarchSplit.getSplits(job, numSplits);
  }

  public RecordReader<Writable, Writable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    MonarchRecordReader reader = new MonarchRecordReader(conf);
    reader.initialize(split, conf);
    return reader;
  }

  public void configure(JobConf job) {
    this.conf = job;
  }
}
