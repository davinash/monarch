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

import static java.lang.ClassLoader.getSystemResource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * Created on: 2015-12-03
 * Since version: 0.2.0
 */
public class TestHelper {
  /**
   * Helper method to read contents of a resource file.
   *
   * @param path the file path for resource
   * @return the list of lines present in the resource file
   * @throws IOException
   */
  public static List<String> getResourceAsString(final String path) throws IOException {
    return FileUtils.readLines(new File(getSystemResource(path).getPath()), "UTF8");
  }

  /**
   * Provide the required configuration object.
   *
   * @return the configuration
   * @param taskId the task identifier
   * @param blockSize the block-size
   */
  public static Configuration getConfiguration(final String regionName, final String locatorPort,
                                               final int blockSize, final String taskId, final String tableType) {
    return new Configuration() {{
      set(MonarchUtils.REGION, regionName);
      set(MonarchUtils.LOCATOR_PORT, locatorPort);
      set(MonarchUtils.BLOCK_SIZE, String.valueOf(blockSize));
      set(MonarchUtils.MONARCH_BATCH_SIZE, String.valueOf(blockSize));
      set("mapreduce.task.id", taskId);
      set(MonarchUtils.MONARCH_TABLE_TYPE, tableType);
    }};
  }

  /**
   * Helper method to write using monarch-record-writer.
   *
   * @param lines the data/records to be written
   * @param conf the writer configuration
   * @return the key-prefix used by the monarch writer
   * @throws IOException
   */
  public static String writeUsingRecordWriter(List<String> lines, Configuration conf) throws IOException {
    final MonarchRecordWriter mrw = new MonarchRecordWriter(conf);
    final BytesWritable bytesWritable = new BytesWritable();
    byte[] bytes;
    for (final String line : lines) {
      bytes = line.getBytes();
      bytesWritable.set(bytes, 0, bytes.length);
      mrw.write(bytesWritable);
    }
    mrw.close(true);
    return mrw.getKeyPrefix();
  }
  @SuppressWarnings("unchecked")
  public static String writeUsingRecordWriter_Array(List<String> lines, Configuration conf) throws IOException {
    final MonarchRecordWriter mrw = new MonarchRecordWriter(conf);
    final BytesRefArrayWritable bytesWritable = new BytesRefArrayWritable(10);
    final Function[] toObject = new Function[]{e -> Long.valueOf(e.toString()), e -> e, e -> Integer
      .valueOf(e.toString()), e -> e};
    final DataType[] objectTypes = new DataType[]{
      BasicTypes.LONG, BasicTypes.STRING, BasicTypes.INT, BasicTypes.STRING
    };
    byte[] bytes;
    String[] cols;
    for (final String line : lines) {
      int i = 0;
      cols = line.split(",");
      bytesWritable.resetValid(cols.length);
      for (final String col : cols) {
        bytes = objectTypes[i].serialize(toObject[i].apply(col));
        bytesWritable.get(i++).set(bytes, 0, bytes.length);
      }
      mrw.write(bytesWritable);
    }
    mrw.close(true);
    return mrw.getKeyPrefix();
  }
  private static final Matcher MATCHER_1 = Pattern.compile("TBLPROPERTIES\\s*\\(.*?\\)",
    Pattern.DOTALL | Pattern.CASE_INSENSITIVE).matcher("dummy");
  private static final Matcher MATCHER_2 = Pattern.compile("\"(.*?)\"\\s*=\\s*\"(.*?)\"",
    Pattern.DOTALL).matcher("dummy");

  /**
   * Replace the properties from TBLPROPERTIES section with the values from the specified
   * map. Everything else is retained as is.
   *
   * @param path the file path
   * @param map the properties to be overridden
   * @return the file content using specified properties, if present
   * @throws IOException if file could not be read
   */
  public static String getResourceAsString1(final String path, final Map<String, String> map) throws IOException {
    String str = FileUtils.readFileToString(new File(getSystemResource(path).getPath()), "UTF8");
    Map<String, String> tmp = new HashMap<>(5);
    final Matcher m1 = MATCHER_1.reset(str);
    StringBuffer sb = new StringBuffer(str.length());
    while (m1.find()) {
      final Matcher m2 = MATCHER_2.reset(m1.group());
      tmp.clear();
      while (m2.find()) {
        for (int i = 1; i <= m2.groupCount()/2; i+=2) {
          tmp.put(m2.group(i), map.getOrDefault(m2.group(i), m2.group(i + 1)));
        }
      }
      final String newStr = tmp.entrySet().stream()
        .map(e -> String.format("\"%s\"=\"%s\"", e.getKey(), e.getValue()))
        .collect(Collectors.joining(", "));
      m1.appendReplacement(sb, "TBLPROPERTIES (" + newStr + ')');
    }
    m1.appendTail(sb);

    return sb.toString();
  }

  public static String getResourceAsString(final String path, final Map<String, String> map) throws IOException {
    String replace = FileUtils.readFileToString(new File(getSystemResource(path).getPath()), "UTF8");
    for (Map.Entry<String, String> entry : map.entrySet())
    {
      replace = replace.replace(entry.getKey(), entry.getValue());
    }
    return replace;
  }
}
