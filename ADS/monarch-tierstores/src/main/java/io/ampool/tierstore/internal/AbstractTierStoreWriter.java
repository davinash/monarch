package io.ampool.tierstore.internal;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.config.CommonConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Logger;


public abstract class AbstractTierStoreWriter implements TierStoreWriter {
  private static final Logger logger = LogService.getLogger();
  protected Properties writerProperties;
  protected ConverterDescriptor converterDescriptor;

  protected static Boolean TIME_BASED_PARTITION = true;


  /**
   * This map stores tablename_partitionid_timeParition -> sequence number. TODO : yet to do reload
   * this map when server or store handler thread is re-initialized
   */
  private static final Map<String, AtomicLong> bucketSeqNoMap = new ConcurrentHashMap<>();

  public AbstractTierStoreWriter() {}

  @Override
  public void init(final Properties properties) {
    this.writerProperties = properties;
  }

  @Override
  public Properties getProperties() {
    return this.writerProperties;
  }

  /**
   * Write multiple rows to file
   *
   * @param writerProperties
   * @param rows
   * @return number of rows written
   */
  @Override
  public int write(final Properties writerProperties, StoreRecord... rows) throws IOException {
    return write(writerProperties, Arrays.asList(rows));
  }

  /**
   * Write multiple rows to file
   *
   * @param writerProperties
   * @param rows
   * @return number of rows written
   */
  @Override
  public int write(final Properties writerProperties, List<StoreRecord> rows) throws IOException {
    Properties newProps = new Properties();
    newProps.putAll(this.writerProperties);
    newProps.putAll(writerProperties);

    String tableName = newProps.getProperty(CommonConfig.TABLE_NAME);
    int partitionId = Integer.parseInt(newProps.getProperty(CommonConfig.PARTITION_ID));
    URI baseURI = URI.create(newProps.getProperty(CommonConfig.BASE_URI));
    final boolean timeBasedPartitioningEnabled = Boolean.parseBoolean(newProps
        .getProperty(CommonConfig.TIME_BASED_PARTITIONING, String.valueOf(TIME_BASED_PARTITION)));
    long timeInterval = 0;
    if (timeBasedPartitioningEnabled) {
      timeInterval = Long.parseLong(newProps.getProperty(CommonConfig.PARTITIIONING_INTERVAL_MS,
          String.valueOf(TierStoreConfiguration.DEFAULT_TIER_PARTITION_INTERVAL_MS)));
    }

    Configuration conf = new Configuration();
    String hdfsSiteXMLPath = newProps.getProperty(CommonConfig.HDFS_SITE_XML_PATH);
    String hadoopSiteXMLPath = newProps.getProperty(CommonConfig.HADOOP_SITE_XML_PATH);
    if (hdfsSiteXMLPath != null) {
      conf.addResource(Paths.get(hdfsSiteXMLPath).toUri().toURL());
    }
    if (hadoopSiteXMLPath != null) {
      conf.addResource(Paths.get(hadoopSiteXMLPath).toUri().toURL());
    }
    newProps.entrySet().forEach((PROP) -> {
      conf.set(String.valueOf(PROP.getKey()), String.valueOf(PROP.getValue()));
    });

    // process records and write

    return _write(newProps, rows, timeInterval, baseURI, tableName, partitionId, conf);
  }

  protected abstract int _write(final Properties props, final List<StoreRecord> rows,
      long timeInterval, URI baseURI, String tableName, int partitionId, Configuration conf)
      throws MalformedURLException;

  @Override
  public void setConverterDescriptor(final ConverterDescriptor converterDescriptor) {
    this.converterDescriptor = converterDescriptor;
  }

  @Override
  public void openChunkWriter(Properties props) throws IOException {
    Properties newProps = new Properties();
    newProps.putAll(this.writerProperties);
    newProps.putAll(props);

    Configuration conf = getConfiguration(newProps);
    String[] tablePathParts = newProps.getProperty(CommonConfig.TABLE_PATH_PARTS)
        .split(CommonConfig.TABLE_PATH_PARTS_SEPARATOR);
    URI baseURI = URI.create(newProps.getProperty(CommonConfig.BASE_URI));

    _openChunkWriter(newProps, baseURI, tablePathParts, conf);
  }

  protected abstract void _openChunkWriter(Properties props, URI baseURI, String[] tablePathParts,
      Configuration conf) throws IOException;

  protected static Configuration getConfiguration(final Properties props) {
    Configuration conf = new Configuration();
    try {
      String hdfsSiteXMLPath = props.getProperty(CommonConfig.HDFS_SITE_XML_PATH);
      String hadoopSiteXMLPath = props.getProperty(CommonConfig.HADOOP_SITE_XML_PATH);
      if (hdfsSiteXMLPath != null) {
        conf.addResource(Paths.get(hdfsSiteXMLPath).toUri().toURL());
      }
      if (hadoopSiteXMLPath != null) {
        conf.addResource(Paths.get(hadoopSiteXMLPath).toUri().toURL());
      }
    } catch (MalformedURLException mue) {
      logger.error("Error while parsing hadoop paths.", mue);
    }
    return conf;
  }

  /**
   * Get the next sequence number for the specified path that includes: table-name, partition/bucket
   * id, time-partition-id
   *
   * @param path the unique path against which the sequence id is maintained
   * @return the next available sequence id
   */
  protected long getNextSeqNo(final String path) {
    synchronized (bucketSeqNoMap) {
      AtomicLong mapValue = bucketSeqNoMap.get(path);
      if (mapValue != null) {
        return mapValue.getAndIncrement();
      }
      bucketSeqNoMap.put(path, new AtomicLong(1));
      return 0;
    }
  }

  /**
   * Return the time-partitioning-key for the specified insertion-time. For all time values that are
   * present in the specified interval, the same key will be generated so that these could be
   * grouped into a single partition (i.e. directory).
   *
   * @param time the insertion time of the record
   * @param interval the ORC partitioning interval
   * @return the partitioning key for the time based on interval
   */
  protected long getTimePartitionKey(final long time, final long interval) {
    return time / interval;
  }
}
