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

package io.ampool.monarch.table.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.ArchiveConfiguration;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.exceptions.TruncateTableException;
import io.ampool.monarch.table.exceptions.UpdateTableException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.FTableExistsException;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.store.ExternalStoreWriter;
import io.ampool.tierstore.TierStoreUtils;
import io.ampool.tierstore.internal.DefaultStore;
import org.apache.geode.CopyHelper;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DestroyTierStoreControllerFunction;
import org.apache.geode.internal.cache.ForceEvictFTableFunction;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.TruncateTableFunction;
import org.apache.geode.internal.cache.UpdateTableFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Implementation of {@link Admin} providing all Administrative operations
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class AdminImpl implements Admin {

  private final MonarchCacheImpl monarchCacheImpl;

  private static final Logger logger = LogService.getLogger();

  public AdminImpl(MonarchCacheImpl mCache) {
    this.monarchCacheImpl = mCache;
  }

  @Override
  public MTable createTable(String tableName, MTableDescriptor inputTableDescriptor) {
    return createMTable(tableName, inputTableDescriptor);
  }

  @Override
  public MTable createMTable(final String tableName, final MTableDescriptor tableDescriptor)
      throws MTableExistsException {
    final Table table = createMemoryTable(tableName, tableDescriptor);
    if (table instanceof MTable || table instanceof ProxyMTableRegion) {
      return (ProxyMTableRegion) table;
    }
    // TODO Confirm if this what expected
    return null;
  }

  private void validateFTableDescriptor(FTableDescriptor tableDescriptor) {
    /** no validation at the moment.. **/
  }

  @Override
  public FTable createFTable(final String tableName, final FTableDescriptor tableDescriptor)
      throws FTableExistsException {
    validateFTableDescriptor(tableDescriptor);
    final Table table = createMemoryTable(tableName, tableDescriptor);
    if (table instanceof FTable || table instanceof ProxyFTableRegion) {
      return (ProxyFTableRegion) table;
    }
    // TODO Confirm if this what expected
    return null;
  }

  @Override
  public Map<String, MTableDescriptor> listTables() {
    Map<String, MTableDescriptor> result = new HashMap<>();
    try {
      Region<String, TableDescriptor> metaRegion =
          (Region<String, TableDescriptor>) monarchCacheImpl
              .getRegion(MTableUtils.AMPL_META_REGION_NAME);

      if (monarchCacheImpl.isClient()) {
        Set<String> tableNames = metaRegion.keySetOnServer();
        Map<String, TableDescriptor> tableMap = metaRegion.getAll(tableNames);
        for (Map.Entry<String, TableDescriptor> entry : tableMap.entrySet()) {
          TableDescriptor td = entry.getValue();
          if (td instanceof MTableDescriptor) {
            result.put(entry.getKey(), (MTableDescriptor) td);
          }
        }
        return result;
      } else {
        for (Map.Entry<String, TableDescriptor> entry : metaRegion.entrySet()) {
          TableDescriptor td = entry.getValue();
          if (td instanceof MTableDescriptor) {
            result.put(entry.getKey(), (MTableDescriptor) td);
          }
        }
        return result;
      }
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Table Listing Failed");
      throw new MCacheInternalErrorException("Table Listing Failed", ge);
    }
  }

  @Override
  public Map<String, MTableDescriptor> listMTables() {
    return listTables();
  }

  @Override
  public Map<String, FTableDescriptor> listFTables() {
    Map<String, FTableDescriptor> result = new HashMap<>();
    try {
      Region<String, TableDescriptor> metaRegion =
          (Region<String, TableDescriptor>) monarchCacheImpl
              .getRegion(MTableUtils.AMPL_META_REGION_NAME);

      if (monarchCacheImpl.isClient()) {
        Set<String> tableNames = metaRegion.keySetOnServer();
        Map<String, TableDescriptor> tableMap = metaRegion.getAll(tableNames);
        for (Map.Entry<String, TableDescriptor> entry : tableMap.entrySet()) {
          TableDescriptor td = entry.getValue();
          if (td instanceof FTableDescriptor) {
            result.put(entry.getKey(), (FTableDescriptor) td);
          }
        }
        return result;
      } else {
        for (Map.Entry<String, TableDescriptor> entry : metaRegion.entrySet()) {
          TableDescriptor td = entry.getValue();
          if (td instanceof FTableDescriptor) {
            result.put(entry.getKey(), (FTableDescriptor) td);
          }
        }
        return result;
      }
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Table Listing Failed");
      throw new MCacheInternalErrorException("Table Listing Failed", ge);
    }
  }

  @Override
  public String[] listTableNames() {
    return listTableNames(false);
  }

  private String[] listTableNames(boolean isFTables) {
    String[] names = null;
    List<String> tableNames = new ArrayList<String>();
    try {
      Region<String, TableDescriptor> metaRegion =
          (Region<String, TableDescriptor>) monarchCacheImpl
              .getRegion(MTableUtils.AMPL_META_REGION_NAME);

      Set<Entry<String, TableDescriptor>> entries;
      if (monarchCacheImpl.isClient()) {
        Map<String, TableDescriptor> values = metaRegion.getAll(metaRegion.keySetOnServer());
        entries = values.entrySet();
      } else {
        entries = metaRegion.entrySet();
      }
      final Iterator<Entry<String, TableDescriptor>> itr = entries.iterator();
      while (itr.hasNext()) {
        final Entry<String, TableDescriptor> entry = itr.next();
        if (isFTables) {
          if (entry.getValue() instanceof FTableDescriptor) {
            tableNames.add(entry.getKey());
          }
        } else {
          if (entry.getValue() instanceof MTableDescriptor) {
            tableNames.add(entry.getKey());
          }
        }
      }
      names = new String[tableNames.size()];
      tableNames.toArray(names);
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Table Listing Failed");
      throw new MCacheInternalErrorException("Table Listing Failed", ge);
    }
    return names;
  }

  @Override
  public String[] listMTableNames() {
    return listTableNames();
  }

  @Override
  public String[] listFTableNames() {
    return listTableNames(true);
  }

  private TableDescriptor getDescriptor(final String tableName) {
    try {
      /* First check table exists on Server or Distributed Systems */
      /* Get the handle of meta region */
      Region metaRegion = monarchCacheImpl.getRegion(MTableUtils.AMPL_META_REGION_NAME);
      if (metaRegion == null) {
        throw new MCacheInternalErrorException("Internal Error, MetaTable not found");
      }
      /* Check if the table exists in Meta Region */
      return (TableDescriptor) metaRegion.get(tableName);
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Internal Error : ", ge);
      throw new MCacheInternalErrorException("Internal Error", ge);
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    try {
      /* First check table exists on Server or Distributed Systems */
      /* Get the handle of meta region */
      Region metaRegion = monarchCacheImpl.getRegion(MTableUtils.AMPL_META_REGION_NAME);
      if (metaRegion == null) {
        throw new MCacheInternalErrorException("Internal Error, MetaTable not found");
      }
      /* Check if the table exists in Meta Region */
      TableDescriptor tableDescriptor = (TableDescriptor) metaRegion.get(tableName);

      /* Check if the table exists in meta region */
      if (tableDescriptor == null) {
        return false;
      }
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Internal Error : ", ge);
      throw new MCacheInternalErrorException("Internal Error", ge);
    }
    return true;
  }

  @Override
  public boolean mTableExists(final String tableName) {
    TableDescriptor td = getDescriptor(tableName);
    return td != null && td instanceof MTableDescriptor;
  }

  @Override
  public boolean existsMTable(String tableName) {
    TableDescriptor td = getDescriptor(tableName);
    return td != null && td instanceof MTableDescriptor;
  }

  @Override
  public boolean fTableExists(final String tableName) {
    TableDescriptor td = getDescriptor(tableName);
    return td != null && td instanceof FTableDescriptor;
  }

  @Override
  public boolean existsFTable(String tableName) {
    TableDescriptor td = getDescriptor(tableName);
    return td != null && td instanceof FTableDescriptor;
  }

  private void deleteInternal(String tableName, Region metaRegion) {
    Region<Object, Object> tableRegion;
    Table anyTable = this.monarchCacheImpl.getAnyTable(tableName);
    if (anyTable == null
        || (tableRegion = ((InternalTable) anyTable).getInternalRegion()) == null) {
      logger.warn("Table " + tableName + " does not exists");
      throw anyTable instanceof FTable
          ? new FTableNotExistsException("Table " + tableName + " does not exists")
          : new MTableNotExistsException("Table " + tableName + " does not exists");
    }

    /*
     * CASE Client 1 creates the table Client 2 tries to delete the table. Client 2 will not have
     * region created internally so do a get table on this client and get the region handle.
     */
    try {
      tableRegion.destroyRegion();
      metaRegion.destroy(tableName);
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Internal Error : ", ge);
      if (ge.getRootCause() instanceof java.io.EOFException) {
        throw new MCacheInternalErrorException("Internal Error");
      } else {
        metaRegion.destroy(tableName);
      }
    }
  }

  @Override
  public void deleteTable(String tableName) {
    /* Get the handle of meta region */
    Region metaRegion = monarchCacheImpl.getRegion(MTableUtils.AMPL_META_REGION_NAME);
    if (metaRegion == null) {
      throw new MCacheInternalErrorException("Internal Error, MetaTable not found");
    }
    TableDescriptor tableDescriptor = null;
    try {
      tableDescriptor = monarchCacheImpl.getTableDescriptor(tableName);
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
    }
    /** to retain backward-compatibility.. since FTable was not there it was always MTable **/
    if (tableDescriptor == null) {
      throw new MTableNotExistsException("Table " + tableName + " does not exists");
    }
    deleteInternal(tableName, metaRegion);
  }

  @Override
  public void deleteMTable(final String tableName) {
    if (!existsMTable(tableName)) {
      throw new MTableNotExistsException("Table " + tableName + " does not exists");
    }
    deleteTable(tableName);
  }

  @Override
  public void deleteFTable(final String tableName) {
    if (!existsFTable(tableName)) {
      throw new FTableNotExistsException("Table " + tableName + " does not exists");
    }
    deleteTable(tableName);
  }

  @Override
  public void truncateTable(String tableName) throws Exception {
    try {
      truncateTable(tableName, null, false);
    } catch (Exception e) {
      logger.error("Truncate Table failed with " + e);
      MTableUtils.checkSecurityException(e);
    }
  }

  @Override
  public void truncateMTable(final String tableName) throws Exception {
    TableDescriptor tableDescriptor = MTableUtils.getTableDescriptor(monarchCacheImpl, tableName);
    if (tableDescriptor instanceof FTableDescriptor)
      throw new IllegalArgumentException("Table " + tableName + " is not a MTable");

    truncateTable(tableName, null, false);
  }

  // INTERNAL
  public Table createMemoryTable(final String tableName,
      final TableDescriptor inputTableDescriptor) {

    if (inputTableDescriptor == null) {
      logger.error("Null table descriptor");
      throw new IllegalArgumentException("The MTableDescriptor cannot be null.");
    }
    if (inputTableDescriptor.getSchema() == null
        && inputTableDescriptor.getColumnDescriptorsMap().size() == 0) {
      logger.error("No schema found: Table should be created with valid schema.");
      throw new IllegalArgumentException("No schema found");
    }
    // if (inputTableDescriptor.getColumnDescriptorsMap().size() == 0) {
    // logger.error("No Column Definition found");
    // throw new IllegalArgumentException("No Column Definition found");
    // }
    MTableUtils.isTableNameLegal(tableName);

    // check if it is ftable
    boolean isFTable = inputTableDescriptor instanceof FTableDescriptor;

    // Creating copy of MtableDescriptor to avoid any side effects
    // from possible updation by user later
    TableDescriptor tableDescriptor = CopyHelper.copy(inputTableDescriptor);

    if (!isFTable && ((MTableDescriptor) tableDescriptor).getUserTable()) {
      if (tableDescriptor.getSchema() == null) {
        if (tableDescriptor.getAllColumnDescriptors().size() != 0) {
          try {
            tableDescriptor.addColumn(MTableUtils.KEY_COLUMN_NAME);
          } catch (TableColumnAlreadyExists ex) {
            // ignore this exception
          }
        }
      } else {
        Schema.Builder sb = new Schema.Builder();
        for (final MColumnDescriptor cd : tableDescriptor.getColumnDescriptors()) {
          sb.column(cd.getColumnNameAsString(), cd.getColumnType());
        }
        sb.column(MTableUtils.KEY_COLUMN_NAME, BasicTypes.BINARY);
        tableDescriptor.setSchema(sb.build());
      }
    }

    // set tablename
    tableDescriptor.setTableName(tableName);

    if (tableDescriptor.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : tableDescriptor.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }

      if (tableDescriptor instanceof FTableDescriptor) {
        /* add insertion-time as last column in the schema.. */
        sb.column(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
        // also add old way
        // TODO improve this way
        tableDescriptor.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
      }
      tableDescriptor.setSchema(sb.build());
    } else {
      if (tableDescriptor instanceof FTableDescriptor) {
        Schema.Builder sb = new Schema.Builder();
        for (final MColumnDescriptor cd : tableDescriptor.getColumnDescriptors()) {
          sb.column(cd.getColumnNameAsString(), cd.getColumnType());
        }
        sb.column(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
        tableDescriptor.setSchema(sb.build());
      }
    }

    if (tableDescriptor instanceof FTableDescriptor) {
      // if no other stores are attached and eviction policy is overflow to tier then set default
      // store
      final FTableDescriptor tempDescriptor = ((FTableDescriptor) tableDescriptor);
      if (tempDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER) {
        Map<String, TierStoreConfiguration> tierStores = tempDescriptor.getTierStores();
        if (tierStores.size() == 0) {
          LinkedHashMap<String, TierStoreConfiguration> tierStoresHierarchy = new LinkedHashMap<>();
          tierStoresHierarchy.put(DefaultStore.STORE_NAME, new TierStoreConfiguration());
          ((FTableDescriptor) tableDescriptor).addTierStores(tierStoresHierarchy);
        }
      }
    }

    Region<String, TableDescriptor> metaRegion = (Region<String, TableDescriptor>) monarchCacheImpl
        .getRegion(MTableUtils.AMPL_META_REGION_NAME);
    try {

      /*
       * Check if table is already deleted from the server Consider Following Case 1. Client-1
       * creates a table 2. Client-2 delete a table 3. Somebody tries to creates a table from
       * Client-1 now we will get table already exists, but meta-region does not have the table, so
       * we need to delete the local region from this new client.
       */
      Region tableRegion = monarchCacheImpl.getRegion(tableName);
      if (this.monarchCacheImpl.isClient() && tableRegion != null) {

        /*
         * Local Region handle for this table is found, but same table is deleted from the
         * MetaRegion. So we need to local delete the region associated with the Table.
         */

        if (metaRegion.get(tableName) == null) {
          tableRegion.localDestroyRegion();
        } else {
          if (isFTable) {
            throw new FTableExistsException("Table " + tableName + " already Exists");
          } else {
            throw new MTableExistsException("Table " + tableName + " already Exists");
          }
        }
      }

      // Step 1
      try {
        tableRegion = monarchCacheImpl.createMemoryTable(tableName, tableDescriptor);
      } catch (MTableExistsException e) {
        logger.error("AdminImpl::createMemoryTable " + e);
        throw e;
      } catch (Exception e) {
        logger.error("AdminImpl::createMemoryTable " + e);
        MTableUtils.checkSecurityException(e);
        throw e;
      }
      if (tableRegion != null) {
        if (!isFTable) {
          ProxyMTableRegion table = null;
          try {
            table = new ProxyMTableRegion(tableRegion, (MTableDescriptor) tableDescriptor,
                this.monarchCacheImpl);
          } catch (GemFireException ge) {
            // Removing entry from metaregion in case of failure
            metaRegion.destroy(tableName);
            logger.error("Table Creation Failed");
            throw new MCacheInternalErrorException("Table Creation Failed", ge);
          }

          return table;
        } else {
          // FTable related
          ProxyFTableRegion table = null;
          try {
            table = new ProxyFTableRegion(tableRegion, (FTableDescriptor) tableDescriptor,
                this.monarchCacheImpl);
          } catch (GemFireException ge) {
            // Removing entry from metaregion in case of failure
            metaRegion.destroy(tableName);
            logger.error("Table Creation Failed");
            throw new MCacheInternalErrorException("Table Creation Failed", ge);
          }
          return table;
        }
      }
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      logger.error("Table Creation Failed");
      throw new MCacheInternalErrorException("Table Creation Failed", ge);
    }
    return null;
  }

  /**
   * Create a store using the given name with fully qualified class name of the store implementation
   * and configuration properties for the store. Store implementations should implement
   * {@link io.ampool.tierstore.TierStore} operations and must provide a empty constructor for
   * initialization.
   *
   * @param name name of the store
   * @param clazz Class which implements the store interface
   * @param storeProps Configuration properties for the Store.
   * @param writerClass Impl of writer(TierStoreWriter) for the store
   * @param writerOpts Configuration properties for the writer.
   * @param readerClass fqcn of a store reader (TierStoreReader)
   * @param readerOpts Configuration properties for the reader.
   */
  public void createTierStore(String name, String clazz, Properties storeProps, String writerClass,
      Properties writerOpts, String readerClass, Properties readerOpts) throws Exception {
    final Iterator<DistributedMember> memberSetItr =
        MTableUtils.getAllDataMembers(monarchCacheImpl).iterator();
    DistributedMember member = null;
    if (memberSetItr.hasNext()) {
      member = memberSetItr.next();
    }
    boolean tierStore = TierStoreUtils.createTierStore(name, clazz, storeProps, writerClass,
        writerOpts, readerClass, readerOpts, member);
    if (tierStore) {
      String successMsg = "Tier store {0} created successfully. " + "Store class: {1}"
          + "Store Properties: {2} " + "Reader class: {3} " + "Reader properties: {4} "
          + "Writer class: {5} " + "Writer properties: {6} ";
      logger.info(CliStrings.format(successMsg, name, clazz, storeProps.toString(), readerClass,
          readerOpts.toString(), writerClass, writerOpts.toString()));
    }
  }

  /**
   * Destroy a tier store
   *
   * @param storeName Name of the store to be destroyed
   */
  public void destroyTierStore(String storeName) throws Exception {
    Function destroyTierStoreControllerFunction = new DestroyTierStoreControllerFunction();
    FunctionService.registerFunction(destroyTierStoreControllerFunction);
    Execution members;

    final Iterator<DistributedMember> memberSetItr =
        MTableUtils.getAllDataMembers(monarchCacheImpl).iterator();
    DistributedMember member = null;
    if (memberSetItr.hasNext()) {
      member = memberSetItr.next();
    }
    members = FunctionService.onMember(member).withArgs(storeName);

    List destroyStoreResult = null;
    Boolean finalRes = true;
    Exception ex = null;
    try {
      destroyStoreResult =
          (ArrayList) members.execute(destroyTierStoreControllerFunction.getId()).getResult();
    } catch (Exception e) {
      // If function execution is failed due to member failure
      // then handle it by rollbacking...
      // Function HA should handle this.
      ex = e;
      finalRes = false;
    }

    if (destroyStoreResult != null) {
      for (int i = 0; i < destroyStoreResult.size(); i++) {
        Object receivedObj = destroyStoreResult.get(i);
        if (receivedObj instanceof Boolean) {
          finalRes = finalRes && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          ex = (Exception) receivedObj;
        }
      }
    }
    if (!finalRes || ex != null) {
      logger.debug("Destroy store for " + storeName + " failed with exception " + ex);
      throw ex;
    }

  }

  /**
   * Delete all records from FTable which match the filter criteria
   *
   * @param tableName Name of the table
   * @param filter Filter for records to be included
   */
  @Override
  public void truncateFTable(String tableName, Filter filter) throws Exception {
    TableDescriptor tableDescriptor = MTableUtils.getTableDescriptor(monarchCacheImpl, tableName);
    if (tableDescriptor instanceof MTableDescriptor)
      throw new IllegalArgumentException("Table " + tableName + " is not an Immutable Table");
    truncateTable(tableName, filter, true);
  }

  @Override
  public void truncateMTable(String tableName, Filter filter, boolean preserveOlderVersions)
      throws Exception {
    TableDescriptor tableDescriptor = MTableUtils.getTableDescriptor(monarchCacheImpl, tableName);
    if (tableDescriptor instanceof FTableDescriptor)
      throw new IllegalArgumentException("Table " + tableName + " is not a MTable");
    truncateTable(tableName, filter, preserveOlderVersions);
  }

  private void validateColumns(Map<byte[], Object> colValues, TableDescriptor td)
      throws IllegalArgumentException {
    Map<MColumnDescriptor, Integer> cdsMap = td.getColumnDescriptorsMap();
    for (Entry<byte[], Object> entry : colValues.entrySet()) {
      if (Bytes.compareTo(entry.getKey(),
          Bytes.toBytes(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) == 0) {
        throw new IllegalArgumentException(
            "Column " + FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME + " can not be updated");
      }
      MColumnDescriptor mcd_in = new MColumnDescriptor(entry.getKey());
      if (!td.getColumnDescriptorsMap().containsKey(mcd_in)) {
        throw new IllegalArgumentException("Column " + Arrays.toString(entry.getKey())
            + " not found in table " + td.getTableName());
      }
    }
  }

  @Override
  public void updateFTable(String tableName, Filter filter, Map<byte[], Object> colValues) {
    Table table = monarchCacheImpl.getFTable(tableName);
    if (table == null) {
      throw new UnsupportedOperationException("FTable " + tableName + " not found");
    }

    TableDescriptor td = table.getTableDescriptor();
    validateColumns(colValues, td);

    Object[] args = new Object[3];
    args[0] = tableName;
    args[1] = filter;
    args[2] = colValues;

    Execution members = null;
    Boolean finalRes = true;
    Exception ex = null;

    try {
      if (monarchCacheImpl.isClient()) {
        members = FunctionService.onServers(monarchCacheImpl.getDefaultPool()).withArgs(args);
      } else {
        members = FunctionService.onMembers(MTableUtils.getAllDataMembers(monarchCacheImpl))
            .withArgs(args);
      }
      List updateTableResult = (ArrayList) members.execute(new UpdateTableFunction()).getResult();

      for (int i = 0; i < updateTableResult.size(); i++) {
        Object receivedObj = updateTableResult.get(i);
        if (receivedObj instanceof Boolean) {
          finalRes = finalRes && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          ex = (Exception) receivedObj;
        }
      }
    } catch (Exception e) {
      logger.error("Exception while truncating table: " + tableName, e);
      MTableUtils.checkSecurityException(e);
      ex = e;
    }
    if (finalRes && ex == null) {
      // success
      logger.info("Updated table: " + tableName);
    } else {
      logger.error("Failed to update table: " + tableName, ex);
      if (ex != null && finalRes == false) {
        throw new UpdateTableException(ex.getMessage());
      } else if (ex != null && finalRes) {
        throw new UpdateTableException(ex.getMessage());
      } else if (ex == null && finalRes == false) {
        throw new UpdateTableException(
            "Failed to update table: " + "Function execution exception, see log for details.");
      }
    }

  }

  @Override
  public void forceFTableEviction(String tableName) {

    Table table = monarchCacheImpl.getFTable(tableName);
    if (table == null) {
      throw new UnsupportedOperationException("FTable " + tableName + " not found");
    }
    Object[] args = new Object[1];
    args[0] = tableName;
    Execution members = null;
    Boolean finalRes = true;
    Exception ex = null;

    try {
      if (monarchCacheImpl.isClient()) {
        members = FunctionService.onServers(monarchCacheImpl.getDefaultPool()).withArgs(args);
      } else {
        members = FunctionService.onMembers(MTableUtils.getAllDataMembers(monarchCacheImpl))
            .withArgs(args);
      }
      List updateTableResult =
          (ArrayList) members.execute(new ForceEvictFTableFunction()).getResult();

      for (int i = 0; i < updateTableResult.size(); i++) {
        Object receivedObj = updateTableResult.get(i);
        if (receivedObj instanceof Boolean) {
          finalRes = finalRes && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          ex = (Exception) receivedObj;
        }
      }
    } catch (Exception e) {
      logger.error("Exception while flushing table: " + tableName, e);
      MTableUtils.checkSecurityException(e);
      ex = e;
    }

  }



  public void truncateTable(String tableName, Filter filter, boolean preserveOlderVersions)
      throws Exception {
    Exception ex = null;
    Region<String, TableDescriptor> metaRegion = (Region<String, TableDescriptor>) monarchCacheImpl
        .getRegion(MTableUtils.AMPL_META_REGION_NAME);

    /* TODO: Check if filters supplied are acceptable */

    /*
     * Get all servers serving the region and execute function on those servers If a bucket moves
     * while the operation is in progress then retry the operation on the new hosting server
     */
    monarchCacheImpl.getTable(tableName);
    Object[] args = new Object[3];
    args[0] = tableName;
    args[1] = filter;
    args[2] = preserveOlderVersions;
    Boolean finalRes = true;
    Execution members = null;
    try {
      if (monarchCacheImpl.isClient()) {
        members = FunctionService.onServers(monarchCacheImpl.getDefaultPool()).withArgs(args);
      } else {
        members = FunctionService.onMembers(MTableUtils.getAllDataMembers(monarchCacheImpl))
            .withArgs(args);
      }
      List truncateTableResult =
          (ArrayList) members.execute(new TruncateTableFunction()).getResult();

      for (int i = 0; i < truncateTableResult.size(); i++) {
        Object receivedObj = truncateTableResult.get(i);
        if (receivedObj instanceof Boolean) {
          finalRes = finalRes && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          ex = (Exception) receivedObj;
        }
      }
    } catch (Exception e) {
      logger.error("Exception while truncating table: " + tableName, e);
      MTableUtils.checkSecurityException(e);
      ex = e;
    }
    if (finalRes && ex == null) {
      // success
      logger.info("Truncated table: " + tableName);
    } else {
      logger.error("Failed to truncate table: " + tableName, ex);
      if (ex != null && finalRes == false) {
        throw new TruncateTableException(ex.getMessage() + "Get status from finalRes");
      } else if (ex != null && finalRes) {
        throw new TruncateTableException(ex.getMessage());
      } else if (ex == null && finalRes == false) {
        throw new TruncateTableException(""/* Find status */);
      }
    }
  }

  /**
   * archives data from the table specified to an external store
   *
   * @param tableName tablename to archive
   * @param configuration archive configuration including filter, file path etc.
   */

  @Override
  public void archiveTable(String tableName, Filter filter, ArchiveConfiguration configuration) {
    TableDescriptor tableDescriptor = monarchCacheImpl.getTableDescriptor(tableName);
    if (tableDescriptor instanceof MTableDescriptor) {
      archiveMTable((MTableDescriptor) tableDescriptor, filter, configuration);
    } else {
      archiveFTable((FTableDescriptor) tableDescriptor, filter, configuration);
    }

  }

  private void archiveMTable(MTableDescriptor tableDescriptor, Filter filter,
      ArchiveConfiguration configuration) {
    MTable mTable = monarchCacheImpl.getMTable(tableDescriptor.getTableName());

    // retrieve the records and write the records
    Scanner scanner = mTable.getScanner(new Scan().setFilter(filter));
    Row[] rows = null;
    try {
      do {
        rows = scanner.next(configuration.getBatchSize());
        if (rows.length != 0) {
          writeRecords(rows, tableDescriptor, configuration);
        }
      } while (rows.length != 0);
    } catch (Exception e) {
      logger.error("Failed to write to external store. Reason is " + e);
      return;
    } finally {
      scanner.close();
    }

    // if writing successful then truncate the records
    try {
      monarchCacheImpl.getAdmin().truncateMTable(tableDescriptor.getTableName(), filter, false);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void archiveFTable(FTableDescriptor tableDescriptor, Filter filter,
      ArchiveConfiguration configuration) {
    FTable fTable = monarchCacheImpl.getFTable(tableDescriptor.getTableName());

    // retrieve the records and write the records
    Scanner scanner = fTable.getScanner(new Scan().setFilter(filter));
    Row[] rows = null;
    try {
      do {
        rows = scanner.next(configuration.getBatchSize());
        if (rows.length != 0) {
          writeRecords(rows, tableDescriptor, configuration);
        }
      } while (rows.length != 0);
    } catch (Exception e) {
      logger.error("Failed to write to external store. Reason is " + e);
      return;
    } finally {
      scanner.close();
    }

    // if writing successful then truncate the records
    try {
      monarchCacheImpl.getAdmin().truncateFTable(tableDescriptor.getTableName(), filter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeRecords(Row[] rows, TableDescriptor tableDescriptor,
      ArchiveConfiguration configuration) throws Exception {
    ExternalStoreWriter externalStoreWriter = configuration.getExternalStoreWriter();
    if (externalStoreWriter != null) {
      externalStoreWriter.write(tableDescriptor, rows, configuration);
    } else {
      throw new RuntimeException("No external store writer found");
    }
  }
}
