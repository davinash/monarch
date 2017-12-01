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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * The Monarch output format handler for Hive.
 *
 */
public class MonarchOutputFormat implements OutputFormat<NullWritable, Writable>, HiveOutputFormat<NullWritable, Writable> {

  public MonarchOutputFormat() {
  }

  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable> getRecordWriter(
    FileSystem ignored, JobConf job, String name, Progressable progress)
    throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }

  protected void validateConfiguration(Configuration conf)
    throws InvalidJobConfException {
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf job)
    throws IOException {
    validateConfiguration(job);
  }

  /**
   * Provide the RecordWriter to the hive for writing data into Monarch.
   *
   * @param jobConf the hadoop job configuration
   * @param path the file path
   * @param aClass the writable class
   * @param b a boolean value
   * @param properties the properties
   * @param progress the progress indicator
   * @return the record-writer for the monarch
   * @throws IOException
   */
  public RecordWriter getHiveRecordWriter
    (JobConf jobConf, Path path, Class<? extends Writable> aClass,
     boolean b, Properties properties, Progressable progress)
    throws IOException {
    return new MonarchRecordWriter(jobConf);
  }
}
