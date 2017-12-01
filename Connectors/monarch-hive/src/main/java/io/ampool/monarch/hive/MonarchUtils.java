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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.functions.MonarchGetWithFilterFunction;
import io.ampool.monarch.functions.NonNullResultCollector;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.security.SecurityConfigurationKeysPublic;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The helper class for utility interaction with Monarch.
 * <p>
 * Created on: 2015-11-02
 * Since version: 0.2.0
 */
public class MonarchUtils {
  public static final String REGION = "monarch.region.name";
  public static final String LOCATOR_HOST = "monarch.locator.host";
  public static final String LOCATOR_HOST_DEFAULT = "localhost";
  public static final String LOCATOR_PORT = "monarch.locator.port";
  public static final int LOCATOR_PORT_DEFAULT = 10334;
  public static final String SPLIT_SIZE_KEY = "monarch.split.size";
  public static final String MONARCH_BATCH_SIZE = "monarch.batch.size";
  public static final String BLOCK_SIZE = "monarch.block.size";
  public static final int DEFAULT_BLOCK_SIZE = 1_000;
  public static final int MONARCH_BATCH_SIZE_DEFAULT = 1000;

  public static final String MONARCH_TABLE_TYPE = "monarch.table.type";
  public static final String DEFAULT_TABLE_TYPE = "immutable";
  public static final String TABLE_TYPE_ORDERED = "ordered";
  public static final String TABLE_TYPE_UNORDERED = "unordered";
  /* applicable only for ordered-table */
  public static final String KEY_RANGE_START = "monarch.key.range.start";
  public static final String KEY_RANGE_STOP = "monarch.key.range.stop";

  public static final String MONARCH_PARTITIONING_COLUMN = "monarch.partitioning.column";

  public static final String REDUNDANCY = "monarch.redundancy";
  public static final int REDUNDANCY_DEFAULT = 0;

  public static final String BUCKETS = "monarch.buckets";
  public static final int BUCKETS_DEFAULT = 113;

  public static final String BLOCK_FORMAT = "monarch.block.format";
  public static final String DEFAULT_BLOCK_FORMAT = "AMP_BYTES";

  // Persistence properties for SOUTH
  public static final String PERSIST = "monarch.enable.persistence";
  public static final String READ_TIMEOUT = "monarch.read.timeout";
  public static final String READ_TIMEOUT_DEFAULT = "120000";

  public static final String MAX_VERSIONS = "monarch.max.versions";
  public static final String MAX_VERSIONS_DEFAULT = "1";
  public static final String READ_MAX_VERSIONS = "monarch.read.max.versions";
  public static final String READ_FILTER_ON_LATEST_VERSION = "monarch.read.filter.latest";
  public static final String READ_OLDEST_FIRST = "monarch.read.oldest.first";
  public static final String WRITE_KEY_COLUMN = "monarch.write.key.column";

  //security properties
  public static final String ENABLE_KERBEROS_AUTHC = "security.enable.kerberos.authc";
  public static final String KERBEROS_SERVICE_PRINCIPAL = "security.kerberos.service.principal";
  public static final String KERBEROS_USER_PRINCIPAL = "security.kerberos.user.principal";
  public static final String KERBEROS_USER_KEYTAB = "security.kerberos.user.keytab";

  public static final String KEY_BLOCKS_SFX = "blocks";
  /** to convert nano-seconds to milli-seconds **/
  public static final int NS_TO_SEC = 1000000000;

  public static final String META_TABLE_SFX = "_meta";
  public static final List<String> META_TABLE_COL_LIST = Collections.singletonList("Data");
  public static final String META_TABLE_COL = "Data";

  private static AmpoolClient aClient = null;
  private static final Logger logger = LoggerFactory.getLogger(MonarchUtils.class);

  public static AmpoolClient getConnectionFromConf(Configuration conf) {
    String locatorHost = conf.get(MonarchUtils.LOCATOR_HOST);
    if (locatorHost == null) {
      locatorHost = MonarchUtils.LOCATOR_HOST_DEFAULT;
    }

    int locatorPort;
    String port = conf.get(MonarchUtils.LOCATOR_PORT);
    if (port == null) {
      locatorPort  = MonarchUtils.LOCATOR_PORT_DEFAULT;
    }
    else {
      locatorPort = Integer.parseInt(port);
    }
    final String readTimeout = conf.get(MonarchUtils.READ_TIMEOUT, MonarchUtils.READ_TIMEOUT_DEFAULT);
    final Properties props = new Properties();
    props.setProperty(io.ampool.conf.Constants.MClientCacheconfig.MONARCH_CLIENT_READ_TIMEOUT, readTimeout);

    //set security properties
    final boolean enableKerberosAuthc = conf.getBoolean(ENABLE_KERBEROS_AUTHC, false);
    if(enableKerberosAuthc){
      final String servicePrincipal = conf.get(KERBEROS_SERVICE_PRINCIPAL);
      final String userPrincipal = conf.get(KERBEROS_USER_PRINCIPAL);
      final String userKeytab = conf.get(KERBEROS_USER_KEYTAB);

      props.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
          "io.ampool.security.AmpoolAuthInitClient.create");
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL, servicePrincipal);
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME, userPrincipal);
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD, userKeytab);
      props.setProperty(SecurityConfigurationKeysPublic.ENABLE_KERBEROS_AUTHC, "true");
    }
    return aClient = new AmpoolClient(locatorHost, locatorPort, props);
  }

  public static MTable getMetaTableInstance(Table table) {
    // MetaTable creation
    if (table != null) {
      Admin admin = aClient.getAdmin();
      if (!aClient.existsMTable(table.getName() + MonarchUtils.META_TABLE_SFX)) {
        MTableDescriptor metatableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
        metatableDescriptor.setSchema(new Schema(
          new String[]{MonarchUtils.META_TABLE_COL}, new DataType[]{BasicTypes.STRING}));
        metatableDescriptor.setUserTable(false);
        return admin.createMTable(table.getName() + MonarchUtils.META_TABLE_SFX, metatableDescriptor);
      } else {
        return aClient.getMTable(table.getName() + MonarchUtils.META_TABLE_SFX);
      }
    }
    return null;
  }

  public static MTable getTableInstance(Configuration conf, String[] cols) {
    aClient = getConnectionFromConf(conf);
    String tableName = conf.get(REGION);
    MTable table = aClient.getMTable(tableName);
    if (table == null) {
      boolean persist;
      String persistence = conf.get(MonarchUtils.PERSIST);
      persist = persistence != null;
      final String tableType = conf.get(MonarchUtils.MONARCH_TABLE_TYPE);
      final MTableType type = TABLE_TYPE_ORDERED.equalsIgnoreCase(tableType)
        ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED;

      MTableDescriptor tableDescriptor = new MTableDescriptor(type);
      int redundancy = NumberUtils.toInt(conf.get(MonarchUtils.REDUNDANCY),
        MonarchUtils.REDUNDANCY_DEFAULT);
      tableDescriptor.setRedundantCopies(redundancy);

      int buckets = NumberUtils.toInt(conf.get(MonarchUtils.BUCKETS),
        MonarchUtils.BUCKETS_DEFAULT);
      tableDescriptor.setTotalNumOfSplits(buckets);

      tableDescriptor.setSchema(new Schema(cols));

      if (persist) {
        if (persistence.compareToIgnoreCase("sync") == 0) {
          tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
        } else if (persistence.compareToIgnoreCase("async") == 0) {
          tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
        }
      }
      Admin admin = aClient.getAdmin();
      table = admin.createMTable(tableName, tableDescriptor);
    }
    return table;
  }

  public static FTable getFTableInstance(Configuration conf, String[] cols) {
    aClient = getConnectionFromConf(conf);
    String tableName = conf.get(REGION);
    FTable table = aClient.getFTable(tableName);
    if (table == null) {
      FTableDescriptor tableDescriptor = new FTableDescriptor();
      int redundancy = NumberUtils.toInt(conf.get(MonarchUtils.REDUNDANCY),
        MonarchUtils.REDUNDANCY_DEFAULT);
      tableDescriptor.setRedundantCopies(redundancy);

      int buckets = NumberUtils.toInt(conf.get(MonarchUtils.BUCKETS),
        MonarchUtils.BUCKETS_DEFAULT);
      tableDescriptor.setTotalNumOfSplits(buckets);

      tableDescriptor.setSchema(new Schema(cols));

      Admin admin = aClient.getAdmin();

      String partitionColumn = conf.get(MonarchUtils.MONARCH_PARTITIONING_COLUMN);
      if (partitionColumn != null) {
        tableDescriptor.setPartitioningColumn(partitionColumn);
      }
      final String blockFormat = conf.get(MonarchUtils.BLOCK_FORMAT);
      if (blockFormat != null) {
        tableDescriptor.setBlockFormat(FTableDescriptor.BlockFormat.valueOf(blockFormat.toUpperCase()));
      }
      final String blockSize = conf.get(MonarchUtils.BLOCK_SIZE);
      if (blockSize != null) {
        tableDescriptor.setBlockSize(Integer.getInteger(blockSize, DEFAULT_BLOCK_SIZE));
      }

      table = admin.createFTable(tableName, tableDescriptor);
    }
    return table;
  }

  public static AmpoolClient getConnection(Map<String, String> parameters) {
    String locatorHost = parameters.get(MonarchUtils.LOCATOR_HOST);
    if (locatorHost == null) {
      locatorHost = MonarchUtils.LOCATOR_HOST_DEFAULT;
    }

    int locatorPort;
    String port = parameters.get(MonarchUtils.LOCATOR_PORT);
    if (port == null) {
      locatorPort  = MonarchUtils.LOCATOR_PORT_DEFAULT;
    }
    else {
      locatorPort = Integer.parseInt(port);
    }
    final String readTimeout = parameters.getOrDefault(MonarchUtils.READ_TIMEOUT, MonarchUtils.READ_TIMEOUT_DEFAULT);
    final Properties props = new Properties();
    props.setProperty(io.ampool.conf.Constants.MClientCacheconfig.MONARCH_CLIENT_READ_TIMEOUT, readTimeout);
    //set security properties
    final boolean enableKerberosAuthc = Boolean.valueOf(parameters.getOrDefault(ENABLE_KERBEROS_AUTHC, "false"));
    if(enableKerberosAuthc){
      final String servicePrincipal = parameters.get(KERBEROS_SERVICE_PRINCIPAL);
      final String userPrincipal = parameters.get(KERBEROS_USER_PRINCIPAL);
      final String userKeytab = parameters.get(KERBEROS_USER_KEYTAB);

      props.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
          "io.ampool.security.AmpoolAuthInitClient.create");
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL, servicePrincipal);
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME, userPrincipal);
      props.setProperty(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD, userKeytab);
      props.setProperty(SecurityConfigurationKeysPublic.ENABLE_KERBEROS_AUTHC, "true");
    }
    return aClient = new AmpoolClient(locatorHost, locatorPort, props);
  }

  public static boolean isFTable(Configuration configuration) {
    String tableType = configuration.get(MonarchUtils.MONARCH_TABLE_TYPE);
    return tableType != null && tableType.equalsIgnoreCase(MonarchUtils.DEFAULT_TABLE_TYPE);
  }

  public static void createConnectionAndFTable(String tableName,
                                              Map<String, String> parameters,
                                              boolean isExternal,
                                              String hiveTableName,
                                              Map<String, String> columnInfo) throws Exception {

    aClient = getConnection(parameters);


    String persistence = parameters.get(MonarchUtils.PERSIST);
    if (persistence != null) {
      throw new Exception("Setting Persistence is not allowed on table with type " + DEFAULT_TABLE_TYPE);
    }

    try {
      // Attempt to create the table, taking EXTERNAL into consideration
      Admin admin = aClient.getAdmin();
      if (!aClient.existsFTable(tableName)) {
        if (!isExternal) {
          FTableDescriptor tableDescriptor = new FTableDescriptor();
          int redundancy = NumberUtils.toInt(parameters.get(MonarchUtils.REDUNDANCY),
            MonarchUtils.REDUNDANCY_DEFAULT);
          tableDescriptor.setRedundantCopies(redundancy);
          int buckets = NumberUtils.toInt(parameters.get(MonarchUtils.BUCKETS),
            MonarchUtils.BUCKETS_DEFAULT);
          tableDescriptor.setTotalNumOfSplits(buckets);

          Schema.Builder sb = new Schema.Builder();
          for (Map.Entry<String, String> entry : columnInfo.entrySet())
          {
            DataType MTableType = null;
            try {
              MTableType = MonarchPredicateHandler.getMonarchFieldType(entry.getValue());
            } catch (IllegalArgumentException iae) {
              ///
            }
            sb.column(entry.getKey(), MTableType == null ? BasicTypes.BINARY : MTableType);
          }
          tableDescriptor.setSchema(sb.build());

          String partitionColumn = parameters.get(MonarchUtils.MONARCH_PARTITIONING_COLUMN);
          if (partitionColumn != null) {
            tableDescriptor.setPartitioningColumn(partitionColumn);
          }
          final String blockFormat = parameters.get(MonarchUtils.BLOCK_FORMAT);
          if (blockFormat != null) {
            tableDescriptor.setBlockFormat(FTableDescriptor.BlockFormat.valueOf(blockFormat.toUpperCase()));
          }
          final String blockSize = parameters.get(MonarchUtils.BLOCK_SIZE);
          if (blockSize != null) {
            tableDescriptor.setBlockSize(Integer.getInteger(blockSize, DEFAULT_BLOCK_SIZE));
          }
          FTable table = admin.createFTable(tableName, tableDescriptor);

          // MetaTable creation
          if (table != null) {
            if (!admin.existsMTable(tableName + MonarchUtils.META_TABLE_SFX)) {
              MTableDescriptor metatableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
              metatableDescriptor.setSchema(new Schema(
                new String[]{MonarchUtils.META_TABLE_COL}, new DataType[]{BasicTypes.STRING}));
              metatableDescriptor.setUserTable(false);
              MTable metaTable = admin.createMTable(tableName + MonarchUtils.META_TABLE_SFX, metatableDescriptor);

              String key = "hive-table";
              Get get = new Get(key);
              Row result = metaTable.get(get);
              if (result.isEmpty() || result.getCells().isEmpty() || new String((byte[])result.getCells().get(0).getColumnValue()).equals(hiveTableName)) {
                Put row = new Put(key);
                row.addColumn(MonarchUtils.META_TABLE_COL, hiveTableName == null ? "" : hiveTableName);
                metaTable.put(row);
              } else {
                throw new Exception("Table " + tableName + " already exists"
                  + " within Monarch; use CREATE EXTERNAL TABLE instead to"
                  + " register it in Hive.");

              }
            }
          }
        } else {
          throw new Exception("Monarch table " + tableName +
            " doesn't exist while the table is declared as an external table.");
        }
      } else {
        if (!isExternal) {
          throw new Exception("Table " + tableName + " already exists"
            + " within Monarch; use CREATE EXTERNAL TABLE instead to"
            + " register it in Hive.");
        }
      }
    } catch (IllegalArgumentException |
      MCacheInternalErrorException | IllegalColumnNameException | MTableExistsException e) {
      throw new Exception(e);
    }
  }

  public static void createConnectionAndTable(String tableName,
                                              Map<String, String> parameters,
                                              boolean isExternal,
                                              String hiveTableName,
                                              Map<String, String> columnInfo) throws Exception {

    aClient = getConnection(parameters);

    String partitionColumn = parameters.get(MonarchUtils.MONARCH_PARTITIONING_COLUMN);
    if (partitionColumn != null) {
      throw new Exception("Setting Partition column is only allowed on table with type " + DEFAULT_TABLE_TYPE);
    }

    boolean persist;
    String persistence = parameters.get(MonarchUtils.PERSIST);
    persist = persistence != null;

    try {
      // Attempt to create the table, taking EXTERNAL into consideration
      Admin admin = aClient.getAdmin();
      if (!aClient.existsMTable(tableName)) {
        if (!isExternal) {
          final String tableType = parameters.get(MonarchUtils.MONARCH_TABLE_TYPE);
          final MTableType type = TABLE_TYPE_ORDERED.equalsIgnoreCase(tableType)
            ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED;
          MTableDescriptor tableDescriptor = new MTableDescriptor(type);
          int redundancy = NumberUtils.toInt(parameters.get(MonarchUtils.REDUNDANCY),
            MonarchUtils.REDUNDANCY_DEFAULT);
          tableDescriptor.setRedundantCopies(redundancy);
          int buckets = NumberUtils.toInt(parameters.get(MonarchUtils.BUCKETS),
            MonarchUtils.BUCKETS_DEFAULT);
          tableDescriptor.setTotalNumOfSplits(buckets);
          int maxVersions = NumberUtils.toInt(parameters.getOrDefault(MonarchUtils.MAX_VERSIONS,
              MonarchUtils.MAX_VERSIONS_DEFAULT));
          tableDescriptor.setMaxVersions(maxVersions);
          /* valid only for ordered tables, at the moment */
          if (type == MTableType.ORDERED_VERSIONED) {
            final String kStart = parameters.get(MonarchUtils.KEY_RANGE_START);
            final String kStop = parameters.get(MonarchUtils.KEY_RANGE_STOP);
            if (kStart != null && kStop != null) {
              tableDescriptor.setStartStopRangeKey(kStart.getBytes("UTF-8"), kStop.getBytes("UTF-8"));
            }
          }
          if (persist) {
            if (persistence.compareToIgnoreCase("sync") == 0) {
              tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
            } else if (persistence.compareToIgnoreCase("async") == 0) {
              tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
            }
          }

          Schema.Builder sb = new Schema.Builder();
          for (Map.Entry<String, String> entry : columnInfo.entrySet())
          {
            DataType MTableType = null;
            try {
              MTableType = MonarchPredicateHandler.getMonarchFieldType(entry.getValue());
            } catch (IllegalArgumentException iae) {
              ///
            }
            sb.column(entry.getKey(), MTableType == null ? BasicTypes.BINARY : MTableType);
          }
          tableDescriptor.setSchema(sb.build());

          MTable table = admin.createMTable(tableName, tableDescriptor);

          // MetaTable creation
          if (table != null) {
            if (!admin.existsMTable(tableName + MonarchUtils.META_TABLE_SFX)) {
              MTableDescriptor metatableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
              metatableDescriptor.setSchema(new Schema(
                new String[]{MonarchUtils.META_TABLE_COL}, new DataType[]{BasicTypes.STRING}));
              metatableDescriptor.setUserTable(false);
              MTable metaTable = admin.createMTable(tableName + MonarchUtils.META_TABLE_SFX, metatableDescriptor);

              String key = "hive-table";
              Get get = new Get(key);
              Row result = metaTable.get(get);
              if (result.isEmpty() || result.getCells().isEmpty() || new String((byte[])result.getCells().get(0).getColumnValue()).equals(hiveTableName)) {
                Put row = new Put(key);
                row.addColumn(MonarchUtils.META_TABLE_COL, hiveTableName == null ? "" : hiveTableName);
                metaTable.put(row);
              } else {
                throw new Exception("Table " + tableName + " already exists"
                        + " within Monarch; use CREATE EXTERNAL TABLE instead to"
                        + " register it in Hive.");

              }
            }
          }
        } else {
          throw new Exception("Monarch table " + tableName +
                  " doesn't exist while the table is declared as an external table.");
        }
      } else {
        if (!isExternal) {
          throw new Exception("Table " + tableName + " already exists"
                  + " within Monarch; use CREATE EXTERNAL TABLE instead to"
                  + " register it in Hive.");
        }
      }
    } catch (IllegalArgumentException |
      MCacheInternalErrorException | IllegalColumnNameException | MTableExistsException e) {
      throw new Exception(e);
    }
  }

  public static void destroyTable(String tableName,
                                  Map<String, String> parameters,
                                  boolean isExternal,
                                  boolean deleteData) throws Exception{

    if (!isExternal) {
      if (deleteData) {

        aClient = getConnection(parameters);
        Admin admin = aClient.getAdmin();
        try {
          admin.deleteMTable(tableName);
          admin.deleteMTable(tableName + MonarchUtils.META_TABLE_SFX);
        } catch (MTableNotExistsException ex) {
          logger.error("Monarch region not present on server. ");
        }
        catch (MCacheInternalErrorException ex) {
          logger.error("Failed to delete table.", ex);
          throw new Exception(ex.getMessage());
        }
      }
    }
  }

  public static void destroyFTable(String tableName,
                                   Map<String, String> parameters,
                                   boolean isExternal,
                                   boolean deleteData) throws Exception {

    if (!isExternal) {
      if (deleteData) {

        aClient = getConnection(parameters);
        Admin admin = aClient.getAdmin();
        try {
          admin.deleteFTable(tableName);
          admin.deleteMTable(tableName + MonarchUtils.META_TABLE_SFX);
        } catch (FTableNotExistsException ex) {
          logger.error("Monarch region not present on server. ");
        } catch (MTableNotExistsException ex) {
          logger.error("Monarch meta region not present on server. ");
        }catch (MCacheInternalErrorException ex) {
          logger.error("Failed to delete table.", ex);
          throw new Exception(ex.getMessage());
        }
      }
    }
  }

//  /**
//   * Put a new value into an entry in the region with the specified key.
//   * @param region the region object
//   * @param key    a key associated with the value to be put in the region
//   * @param value  the value to be put into the cache of the region
//   */
  public static void putInTable(final MTable table,
                                 final Put put) {
    if (table != null) {
      table.put(put);
    } else {
      logger.error("table is null ");
    }
  }

  public static final String FMT = "%-16s: Count= %,10d, TotalSecs= %,10.3f, SecsPerCount= %10.3f";
  public static final String FMT1 = "%-16s: TotalBytes= %5s, TotalSecs= %,.3f, BytesPerSec= %s";
  public static String getTimeString(final String msg, final long totalCount, final long nanoSecs) {
    double secs = ((double)nanoSecs) / NS_TO_SEC;
    return String.format(FMT, msg, totalCount, secs, totalCount > 0 ? (secs / totalCount) : 0);
  }
  public static String getDataRateString(final String msg, final long byteCount, final long nanoSecs) {
    double secs = (double)nanoSecs/ NS_TO_SEC;
    return String.format(FMT1, msg,
      FileUtils.byteCountToDisplaySize(byteCount), secs,
      FileUtils.byteCountToDisplaySize(secs > 0 ? (long)(byteCount / secs) : 0));
  }

  /**
   * Get the results using {@link MonarchGetWithFilterFunction} via function execution.
   * <p>
   * Get matching rows from MTable using function-execution. The pushDownPredicates is an
   * array of predicates tested before the rows are returned. If there are no predicates
   * specified all rows are returned else only rows satisfying predicates will be returned.
   * <p>
   *
   * @param mTable the MTable where function should be executed
   * @param keySet the keys to be retrieved from the region
   * @param args   arguments to be passed to the executing function
   * @return the list of matching rows (i.e. list of cells)
   */
  @SuppressWarnings("unchecked")
  public static Collection<Object> getWithFilter(final MTable mTable, final Set<MTableKey> keySet,
                                                 final Object[] args) {
    Object output = FunctionService
      .onRegion(((ProxyMTableRegion) mTable).getTableRegion())
      .withCollector(new NonNullResultCollector())
      .withFilter(keySet)
      .withArgs(args)
      .execute(new MonarchGetWithFilterFunction()).getResult();
    assert output instanceof List;
    return (List<Object>) output;
  }
}
