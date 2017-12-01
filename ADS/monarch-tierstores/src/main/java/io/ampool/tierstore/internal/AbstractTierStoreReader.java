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

package io.ampool.tierstore.internal;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.config.CommonConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


public abstract class AbstractTierStoreReader implements TierStoreReader {
  private static Boolean TIME_BASED_PARTITION = true;
  private static long TIME_BASED_PARTITION_INTERVAL = 600_000_000_000L;

  private Properties properties;
  protected String[] fileList = null;
  // TODO: Need to replace with tierstore scanner .
  protected StoreScan scan = null;
  protected ConverterDescriptor converterDescriptor;
  private String tablePathParts = null;
  protected String tableName = null;
  protected int partitionId = -1;
  protected int fileListIndex = 0;
  private ReaderOptions readerOptions;



  private String directory;
  protected java.nio.file.Path partPath;
  protected Configuration conf;


  @Override
  public void init(final Properties properties) {
    this.properties = properties;
    this.fileList = null;
    if (this.scan == null) {
      this.scan = new StoreScan();
    }
  }


  @Override
  public Properties getProperties() {
    return this.properties;
  }

  @Override
  public void setFilter(final StoreScan scan) {
    if (scan == null) {
      // to safeguard against NPE
      this.scan = new StoreScan();
    } else {
      this.scan = scan;
    }
  }

  protected void readProperties() {
    this.tablePathParts = this.properties.getProperty(CommonConfig.TABLE_PATH_PARTS);
    this.tableName = this.properties.getProperty(CommonConfig.TABLE_NAME);
    final String partitionIdStr = this.properties.getProperty(CommonConfig.PARTITION_ID);
    if (partitionIdStr != null) {
      this.partitionId = Integer.parseInt(partitionIdStr);
    }
    final URI baseURI = URI.create(this.properties.getProperty(CommonConfig.BASE_URI));
    this.directory = baseURI.getPath();
    this.partPath = tablePathParts != null
        ? Paths.get(this.directory, tablePathParts.split(CommonConfig.TABLE_PATH_PARTS_SEPARATOR))
        : Paths.get(this.directory, tableName, String.valueOf(partitionId));
    this.conf = new Configuration();

    String hdfsSiteXMLPath = this.properties.getProperty(CommonConfig.HDFS_SITE_XML_PATH);
    String hadoopSiteXMLPath = this.properties.getProperty(CommonConfig.HADOOP_SITE_XML_PATH);
    try {
      if (hdfsSiteXMLPath != null) {
        conf.addResource(Paths.get(hdfsSiteXMLPath).toUri().toURL());
      }
      if (hadoopSiteXMLPath != null) {
        conf.addResource(Paths.get(hadoopSiteXMLPath).toUri().toURL());
      }
      properties.entrySet().forEach((PROP) -> {
        conf.set(String.valueOf(PROP.getKey()), String.valueOf(PROP.getValue()));
      });
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    this.fileList = null;

    if (this.scan == null) {
      this.scan = new StoreScan();
    }
  }


  /**
   * Get the required files to scan for the respective table and partition using the specified time
   * ranges. In case the time ranges are not specified all the matching ORC files would be returned.
   * With valid time ranges, only ORC files from the matching time-partitions would be returned.
   *
   * @param tableName the table name
   * @param partitionId the partition/bucket id
   * @param ranges the list of time partition ranges
   * @return the list of matching ORC files that are to be scanned
   */
  protected String[] getFilesToScan(String tableName, int partitionId,
      List<Pair<Long, Long>> ranges, final String fileExt) {

    final boolean timeBasedPartitioningEnabled = Boolean.parseBoolean(this.properties
        .getProperty(CommonConfig.TIME_BASED_PARTITIONING, String.valueOf(TIME_BASED_PARTITION)));
    final long timeInterval =
        Long.parseLong(this.properties.getProperty(CommonConfig.PARTITIIONING_INTERVAL_MS,
            String.valueOf(TIME_BASED_PARTITION_INTERVAL)));

    if (ranges == null || ranges.isEmpty()) {
      return tablePathParts != null
          ? getOrcFiles(this.directory, fileExt,
              tablePathParts.split(CommonConfig.TABLE_PATH_PARTS_SEPARATOR))
          : getOrcFiles(this.directory, fileExt, tableName, String.valueOf(partitionId));
    } else {
      File topDir =
          Paths.get(this.directory, this.tableName, String.valueOf(this.partitionId)).toFile();
      List<Pair<Long, Long>> r = ranges.stream().map((range) -> {
        return new Pair<>(range.getFirst() / timeInterval, range.getSecond() / timeInterval);
      }).collect(Collectors.toList());

      return getOrcFilesForRange(topDir, r, fileExt);
    }
  }

  /**
   * Get the list of all ORC files from the specified directory where the respective time partitions
   * fall in at least one of the specified range. The time partition directories that are outside of
   * the ranges are ignored for optimal execution of query.
   *
   * @param topDir the top-level directory where the table+partition/bucket is stored
   * @param ranges the time range to be used for retrieving matching ORC files
   * @return the list of matching ORC files
   */
  public static String[] getOrcFilesForRange(final File topDir, final List<Pair<Long, Long>> ranges,
      final String fileExt) {
    File[] dirs = topDir.listFiles();
    if (dirs == null || dirs.length == 0) {
      return new String[0];
    } else {
      return Stream.of(dirs).filter(e -> {
        final long l = Long.parseLong(e.getName());
        return ranges.stream().anyMatch(x -> l >= x.getFirst() && l <= x.getSecond());
      }).sorted(
          (o1, o2) -> Long.compare(Long.parseLong(o1.getName()), Long.parseLong(o2.getName())))
          .map(d -> d.listFiles((dir, name) -> !name.endsWith(fileExt)))
          .flatMap(a -> Arrays.stream(a).sorted(
              (f1, f2) -> Long.compare(Long.parseLong(f1.getName()), Long.parseLong(f2.getName())))
              .map(f -> Paths.get(f.getParentFile().getName(), f.getName()).toString()))
          .toArray(String[]::new);
    }
  }

  /**
   * Get all ORC files present in directory for the specified table and partition/bucket. The ORC
   * files returned are in ascending order of the (insertion) time-partition and sequence-id within
   * the time-partition.
   *
   * @param orcDir the ORC store directory
   * @param args the arguments in order: table-name, bucket-id, time-partition-id
   * @return the list of all ORC files
   */
  private String[] getOrcFiles(final String orcDir, final String fileExt, final String... args) {
    try {
      FileSystem fileSystem = FileSystem.get(conf);
      Path distributedPath = new Path(Paths.get(orcDir, args).toString());
      ArrayList<String> filePathStrings = new ArrayList<>();
      if (fileSystem.exists(distributedPath)) {
        RemoteIterator<LocatedFileStatus> fileListItr = fileSystem.listFiles(distributedPath, true);
        while (fileListItr != null && fileListItr.hasNext()) {
          LocatedFileStatus file = fileListItr.next();
          if (!file.getPath().getName().endsWith(fileExt)) {
            // exclude CRC files
            filePathStrings.add(file.getPath().toUri().toString());
          }
        }

        Collections.sort(filePathStrings);
      }
      String[] retArray = new String[filePathStrings.size()];
      filePathStrings.toArray(retArray);
      return retArray;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new String[0];
  }

  @Override
  public void setConverterDescriptor(final ConverterDescriptor converterDescriptor) {
    this.converterDescriptor = converterDescriptor;
  }

  @Override
  public Iterator<StoreRecord> iterator() {
    return null;
  }

  @Override
  public String toString() {
    return "AbstractTierStoreReader{" + "properties=" + properties + ", fileList="
        + Arrays.toString(fileList) + ", scan=" + scan + ", converterDescriptor="
        + converterDescriptor + ", tablePathParts='" + tablePathParts + '\'' + ", tableName='"
        + tableName + '\'' + ", partitionId=" + partitionId + ", fileListIndex=" + fileListIndex
        + ", directory='" + directory + '\'' + ", partPath=" + partPath + ", conf=" + conf + '}';
  }

  public ReaderOptions getReaderOptions() {
    return readerOptions;
  }

  @Override
  public void setReaderOptions(final ReaderOptions readerOptions) {
    this.readerOptions = readerOptions;
  }
}
