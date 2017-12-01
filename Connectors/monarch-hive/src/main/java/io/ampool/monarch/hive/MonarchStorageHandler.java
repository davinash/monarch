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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Monarch storage handler for Hive.
 *
 */
public class MonarchStorageHandler implements HiveStorageHandler, HiveStoragePredicateHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MonarchStorageHandler.class);

    private Configuration configuration;

    public Class<? extends InputFormat> getInputFormatClass() {
        return MonarchInputFormat.class;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return MonarchOutputFormat.class;
    }

    public Class<? extends AbstractSerDe> getSerDeClass() {
        return MonarchSerDe.class;
    }

    public HiveMetaHook getMetaHook() {
        return new MonarchMetaHook();
    }

    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {

        Properties tableProperties = tableDesc.getProperties();

        String regionName = tableProperties.getProperty(MonarchUtils.REGION);
        if (regionName == null) {
            // 1. If region name property is not provided use hive's table name.
            // 2. This gives dbName.tableName, so replace '.'(dot) with '_'(underscore) since gemfire
            // does not allow querying when region name contains dot.
            regionName = tableDesc.getTableName().replace('.', '_');
        }
        map.put(MonarchUtils.REGION, regionName);

        String host = tableProperties.getProperty(MonarchUtils.LOCATOR_HOST);
        if (host == null) {
            host = tableProperties.getProperty(MonarchUtils.LOCATOR_HOST, MonarchUtils.LOCATOR_HOST_DEFAULT);
        }
        map.put(MonarchUtils.LOCATOR_HOST, host);

        String port = tableProperties.getProperty(MonarchUtils.LOCATOR_PORT);
        if (port== null) {
            port = tableProperties.getProperty(MonarchUtils.LOCATOR_PORT, MonarchUtils.LOCATOR_PORT_DEFAULT + "");
        }
        map.put(MonarchUtils.LOCATOR_PORT, port);

        // monarch redundancy, if not set default is 0.
        String redundancy = tableProperties.getProperty(MonarchUtils.REDUNDANCY, MonarchUtils.REDUNDANCY_DEFAULT + "");
        map.put(MonarchUtils.REDUNDANCY, redundancy);

        // monarch buckets, if not set default is 113.
        String buckets = tableProperties.getProperty(MonarchUtils.BUCKETS, MonarchUtils.BUCKETS_DEFAULT + "");
        map.put(MonarchUtils.BUCKETS, buckets);

        // Get all column names and types
        String colNamesList = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
        map.put(serdeConstants.LIST_COLUMNS, colNamesList);

        String colTypeList = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        map.put(serdeConstants.LIST_COLUMN_TYPES, colTypeList);

        /* client-read-timeout */
        map.put(MonarchUtils.READ_TIMEOUT,
          tableProperties.getProperty(MonarchUtils.READ_TIMEOUT, MonarchUtils.READ_TIMEOUT_DEFAULT));

        // Table type property
        String tableType = tableProperties.getProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
        map.put(MonarchUtils.MONARCH_TABLE_TYPE, tableType);

        /* max-versions */
        map.put(MonarchUtils.MAX_VERSIONS,
          tableProperties.getProperty(MonarchUtils.MAX_VERSIONS, MonarchUtils.MAX_VERSIONS_DEFAULT));

        /* start-key and stop-key for ordered table */
        final String start = tableProperties.getProperty(MonarchUtils.KEY_RANGE_START);
        final String stop = tableProperties.getProperty(MonarchUtils.KEY_RANGE_STOP);
        if (start != null && stop != null) {
            map.put(MonarchUtils.KEY_RANGE_START, start);
            map.put(MonarchUtils.KEY_RANGE_STOP, stop);
        }

        //security properties
        boolean enableKerberosAuthc = Boolean.valueOf(tableProperties.getProperty(MonarchUtils.ENABLE_KERBEROS_AUTHC, "false"));
        if(enableKerberosAuthc) {
            map.put(MonarchUtils.ENABLE_KERBEROS_AUTHC, "true");
            map.put(MonarchUtils.KERBEROS_SERVICE_PRINCIPAL, tableProperties.getProperty(MonarchUtils.KERBEROS_SERVICE_PRINCIPAL));
            map.put(MonarchUtils.KERBEROS_USER_PRINCIPAL, tableProperties.getProperty(MonarchUtils.KERBEROS_USER_PRINCIPAL));
            map.put(MonarchUtils.KERBEROS_USER_KEYTAB, tableProperties.getProperty(MonarchUtils.KERBEROS_USER_KEYTAB));
        }

        if (tableType.equalsIgnoreCase(MonarchUtils.DEFAULT_TABLE_TYPE)) {
          String partitionColumn = tableProperties.getProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN);
          if (partitionColumn != null) {
            map.put(MonarchUtils.MONARCH_PARTITIONING_COLUMN, partitionColumn);
          }
          final String blockFormat = tableProperties.getProperty(MonarchUtils.BLOCK_FORMAT);
          if (blockFormat != null) {
            map.put(MonarchUtils.BLOCK_FORMAT, blockFormat);
          }
          final String blockSize = tableProperties.getProperty(MonarchUtils.BLOCK_SIZE);
          if (blockSize != null) {
            map.put(MonarchUtils.BLOCK_SIZE, blockSize);
          }
        } else {
          // Persistence properties
          String persist = tableProperties.getProperty(MonarchUtils.PERSIST);
          if (persist != null) {
            map.put(MonarchUtils.PERSIST, persist);
          }
        }
    }

    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {

        Properties tableProperties = tableDesc.getProperties();

        String regionName = tableProperties.getProperty(MonarchUtils.REGION);
        if (regionName == null) {
            // 1. If region name property is not provided use hive's table name.
            // 2. This gives dbName.tableName, so replace '.'(dot) with '_'(underscore) since gemfire
            // does not allow querying when region name contains dot.
            regionName = tableDesc.getTableName().replace('.', '_');
        }
        map.put(MonarchUtils.REGION, regionName);

        String host = tableProperties.getProperty(MonarchUtils.LOCATOR_HOST);
        if (host == null) {
            host = tableProperties.getProperty(MonarchUtils.LOCATOR_HOST, MonarchUtils.LOCATOR_HOST_DEFAULT);
        }
        map.put(MonarchUtils.LOCATOR_HOST, host);

        String port = tableProperties.getProperty(MonarchUtils.LOCATOR_PORT);
        if (port== null) {
            port = tableProperties.getProperty(MonarchUtils.LOCATOR_PORT, MonarchUtils.LOCATOR_PORT_DEFAULT + "");
        }
        map.put(MonarchUtils.LOCATOR_PORT, port);

        // monarch redundancy, if not set default is 0.
        String redundancy = tableProperties.getProperty(MonarchUtils.REDUNDANCY, MonarchUtils.REDUNDANCY_DEFAULT + "");
        map.put(MonarchUtils.REDUNDANCY, redundancy);

        // monarch buckets, if not set default is 113.
        String buckets = tableProperties.getProperty(MonarchUtils.BUCKETS, MonarchUtils.BUCKETS_DEFAULT + "");
        map.put(MonarchUtils.BUCKETS, buckets);

        // Get all column names and types
        String colNamesList = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
        map.put(serdeConstants.LIST_COLUMNS, colNamesList);

        String colTypeList = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        map.put(serdeConstants.LIST_COLUMN_TYPES, colTypeList);

        /* client-read-timeout */
        map.put(MonarchUtils.READ_TIMEOUT,
          tableProperties.getProperty(MonarchUtils.READ_TIMEOUT, MonarchUtils.READ_TIMEOUT_DEFAULT));

        /* max-versions */
        map.put(MonarchUtils.MAX_VERSIONS,
          tableProperties.getProperty(MonarchUtils.MAX_VERSIONS, MonarchUtils.MAX_VERSIONS_DEFAULT));

        /* start-key and stop-key for ordered table */
        final String start = tableProperties.getProperty(MonarchUtils.KEY_RANGE_START);
        final String stop = tableProperties.getProperty(MonarchUtils.KEY_RANGE_STOP);
        if (start != null && stop != null) {
            map.put(MonarchUtils.KEY_RANGE_START, start);
            map.put(MonarchUtils.KEY_RANGE_STOP, stop);
        }

        //security properties
        boolean enableKerberosAuthc = Boolean.valueOf(tableProperties.getProperty(MonarchUtils.ENABLE_KERBEROS_AUTHC, "false"));
        if(enableKerberosAuthc) {
            map.put(MonarchUtils.ENABLE_KERBEROS_AUTHC, "true");
            map.put(MonarchUtils.KERBEROS_SERVICE_PRINCIPAL, tableProperties.getProperty(MonarchUtils.KERBEROS_SERVICE_PRINCIPAL));
            map.put(MonarchUtils.KERBEROS_USER_PRINCIPAL, tableProperties.getProperty(MonarchUtils.KERBEROS_USER_PRINCIPAL));
            map.put(MonarchUtils.KERBEROS_USER_KEYTAB, tableProperties.getProperty(MonarchUtils.KERBEROS_USER_KEYTAB));
        }

        // Table type property
        String tableType = tableProperties.getProperty(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
        map.put(MonarchUtils.MONARCH_TABLE_TYPE, tableType);

        if (tableType.equalsIgnoreCase(MonarchUtils.DEFAULT_TABLE_TYPE)) {
          String partitionColumn = tableProperties.getProperty(MonarchUtils.MONARCH_PARTITIONING_COLUMN);
          if (partitionColumn != null) {
           map.put(MonarchUtils.MONARCH_PARTITIONING_COLUMN, partitionColumn);
          }
        } else {
          // Persistence properties
          String persist = tableProperties.getProperty(MonarchUtils.PERSIST);
          if (persist != null) {
              map.put(MonarchUtils.PERSIST, persist);
          }
        }
    }

    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {
        // Should not be getting invoked, configureInputJobProperties or configureOutputJobProperties
        // should be invoked instead.
        configureInputJobProperties(tableDesc, map);
        configureOutputJobProperties(tableDesc, map);
    }

    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        // configureTableJobProperties(tableDesc, jobConf);
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    protected boolean isExternalTable(Table table) {
        return MetaStoreUtils.isExternalTable(table);
    }

    /**
     * Get the Monarch TableName.
     *
     * @param tbl  the hive Table containing Monarch tableName.
     * @return the String Monarch TableName
     */
    protected String getMonarchTableName(Table tbl) {
        // 1. If region name property is not provided use hive's table name.
        // 2. Use '_'(underscore) instead of '.'(dot) since gemfire
        // does not allow querying when region name contain dot.
        String tableName = tbl.getParameters().get(MonarchUtils.REGION);
        if (tableName == null) {
            tableName = tbl.getDbName() + "_" + tbl.getTableName();
        }
        return tableName;
    }

    @Override
    public DecomposedPredicate decomposePredicate(final JobConf jobConf,
                                                  final Deserializer deserializer,
                                                  final ExprNodeDesc exprNodeDesc) {
        return MonarchPredicateHandler.decomposePredicate(jobConf, (MonarchSerDe) deserializer, exprNodeDesc);
    }

    private class MonarchMetaHook implements HiveMetaHook {

        /*Test scenario's for MonarchMetaHook's preCreateTable with Hive Table and Monarch Region
        1. hive table = a1 and monarch region = a0 ; success
        2. hive external table = a2 and monarch existing region = a0 ; success
        3. hive table  = a3 and existing monarch region = a0; failure
        4. hive external table = a4 and existing monarch region = a0 ; success
        5. restart hive cli shell and test
        hive existing table  = a1 and existing monarch region = a0 ; failure with deletion of existing monarch region.
                hive new table  = a5 and existing monarch region = a0; failure
        6. hive external table = a6 and no existing monarch region a0 ; failure*/

        public void preCreateTable(Table table) throws MetaException {
            // We want data to be stored in monarch, nowwhere else.
            if (table.getSd().getLocation() != null) {
                throw new MetaException("Location can't be specified for Monarch");
            }

            boolean isExternal = isExternalTable(table);
            String tableName = getMonarchTableName(table);
            String hiveTableName = table.getDbName() + "_" + table.getTableName();

            Map<String, String> columnInfo = new LinkedHashMap<>();

            Iterator<FieldSchema> columnIterator = table.getSd().getColsIterator();
            if (columnIterator != null) {
                while (columnIterator.hasNext()) {
                    FieldSchema e = columnIterator.next();
                    columnInfo.put(e.getName(), e.getType());
                }
            }

            try {
              Map<String, String> parameters = table.getParameters();
              String tableType = parameters.getOrDefault(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
              if (tableType.equalsIgnoreCase(MonarchUtils.DEFAULT_TABLE_TYPE)) {
                MonarchUtils.createConnectionAndFTable(tableName, parameters, isExternal, hiveTableName, columnInfo);
              } else {
                MonarchUtils.createConnectionAndTable(tableName, parameters, isExternal, hiveTableName, columnInfo);
              }
            } catch (Exception se) {
                LOG.error("Failed to create table: {}", tableName, se);
                throw new MetaException(se.getMessage());
            }
        }
        public void rollbackCreateTable(Table table) throws MetaException {
            // Same as commitDropTable where we always delete the data (monarch table)
            // here is no facility for two-phase commit in metadata transactions against the Hive metastore
            // and the storage handler. As a result, there is a small window in which a crash during DDL
            // can lead to the two systems getting out of sync.
            commitDropTable(table, true);
        }

        public void commitCreateTable(Table table) throws MetaException {

        }

        public void preDropTable(Table table) throws MetaException {

        }

        public void rollbackDropTable(Table table) throws MetaException {

        }

        public void commitDropTable(Table table, boolean deleteData) throws MetaException {

            boolean isExternal = isExternalTable(table);
            String tableName = getMonarchTableName(table);
            try {
                Map<String, String> parameters = table.getParameters();
                String tableType = parameters.getOrDefault(MonarchUtils.MONARCH_TABLE_TYPE, MonarchUtils.DEFAULT_TABLE_TYPE);
                if (tableType.equalsIgnoreCase(MonarchUtils.DEFAULT_TABLE_TYPE)) {
                  MonarchUtils.destroyFTable(tableName, table.getParameters(), isExternal, deleteData);
                } else {
                  MonarchUtils.destroyTable(tableName, table.getParameters(), isExternal, deleteData);
                }
            } catch (Exception se) {
                throw new MetaException(se.getMessage());
            }
        }

    }
}
