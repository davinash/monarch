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

package org.apache.geode.internal.cache;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.functions.DeleteWithFilterFunction;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.BitMap;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableRegionImpl;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.table.internal.RowHeader;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.table.region.ScanCommand;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreCreateException;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreReaderFactory;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.TierStoreWriterFactory;
import io.ampool.tierstore.internal.DefaultStore;
import org.apache.geode.CancelException;
import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MonarchCacheImpl extends GemFireCacheImpl implements MCache, MClientCache {

  private static final Logger logger = LogService.getLogger();

  /**
   * True if this cache is being created by a ClientCacheFactory.
   */
  private final boolean isClient;

  /**
   * the last instance of MonarchCacheImpl created
   */
  private static volatile MonarchCacheImpl instance = null;

  private static Admin admin = null;

  private static boolean isMetaRegionCached = false;

  /**
   * Creates a new instance of GemFireCache and populates it according to the
   * <code>cache.xml</code>, if appropriate.
   * 
   * @param typeRegistry : currently only unit tests set this parameter to a non-null value
   */
  private MonarchCacheImpl(boolean isClient, PoolFactory pf, DistributedSystem system,
      CacheConfig cacheConfig, boolean asyncEventListeners, TypeRegistry typeRegistry) {
    super(isClient, pf, system, cacheConfig, asyncEventListeners, typeRegistry);
    // TODO: super and this are initialized in separate sync blocks
    synchronized (MonarchCacheImpl.class) {
      FunctionService.registerFunction(new MTableCreationFunction());
      FunctionService.registerFunction(new CreateMTableControllerFunction());
      FunctionService.registerFunction(new MTableGetStartEndKeysFunction());
      FunctionService.registerFunction(new CreateMTableFunction());
      FunctionService.registerFunction(new DeleteMTableFunction());
      FunctionService.registerFunction(new DescMTableFunction());
      FunctionService.registerFunction(new MTableDataCommandsFunction());
      FunctionService.registerFunction(new MCountFunction());
      FunctionService.registerFunction(new CreateDiskStoreFunction());
      FunctionService.registerFunction(new CreateTierStoreControllerFunction());
      FunctionService.registerFunction(new TierStoreCreationFunction());
      FunctionService.registerFunction(new TierStoreDeletionFunction());

      this.isClient = isClient;
    }
  }


  public static MonarchCacheImpl getInstance() {
    return instance;
  }

  @Override
  public StoreHandler getStoreHandler() {
    return StoreHandler.getInstance();
  }

  @Override
  public Region getStoreMetaRegion() {
    return getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
  }

  @Override
  public FTableDescriptor getFTableDescriptor(String tableName) {
    return (FTableDescriptor) getTableDescriptor(tableName);
  }

  @Override
  public MTableDescriptor getMTableDescriptor(String tableName) {
    return (MTableDescriptor) getTableDescriptor(tableName);
  }

  @Override
  public TableDescriptor getTableDescriptor(String tableName) {

    try {
      return (TableDescriptor) getRegion(MTableUtils.AMPL_META_REGION_NAME).get(tableName);
    } catch (GemFireException ex) {
      MTableUtils.checkSecurityException(ex);
      throw ex;
    }

  }

  /**
   * register the internal function used for monarch cache.
   */
  private void registerInternalFunctions() {
    FunctionService.registerFunction(new DeleteWithFilterFunction());
  }

  @Override
  public Admin getAdmin() {
    return admin;
  }

  @Override
  public MTable getTable(String tableName) throws IllegalArgumentException {
    return getMTable(tableName);
  }


  public Table getAnyTable(String tableName) throws IllegalArgumentException {
    try {
      TableDescriptor tableDescriptor = getTableDescriptor(tableName);
      if (tableDescriptor != null) {
        Region r = getRegion(tableName);

        if (!isClient) {
          if (r == null) {
            return null;
          }
        }

        if (r == null) {
          try {
            // TODO: Change this to create ServerTableRegionProxy.
            r = createClientRegionFactory(ClientRegionShortcut.PROXY).create(tableName);
          } catch (RegionExistsException ree) {
            r = getRegion(tableName);
          }
        }
        if (tableDescriptor instanceof MTableDescriptor) {
          // return new ProxyMTableRegion(r, (MTableDescriptor) tableDescriptor, this);
          return new ProxyMTableRegion(r, (MTableDescriptor) tableDescriptor, this);
        } else if (tableDescriptor instanceof FTableDescriptor) {
          return new ProxyFTableRegion(r, (FTableDescriptor) tableDescriptor, this);
        }
      }
    } catch (CacheClosedException cce) {
      throw new MCacheInternalErrorException(cce.getMessage());
    }
    return null;
  }

  @Override
  public MTable getMTable(final String tableName) throws IllegalArgumentException {
    final Table table = getAnyTable(tableName);
    if (table instanceof MTable || table instanceof ProxyMTableRegion) {
      return (ProxyMTableRegion) table;
    }
    return null;
  }

  @Override
  public FTable getFTable(final String tableName) throws IllegalArgumentException {
    final Table table = getAnyTable(tableName);
    if (table instanceof FTable || table instanceof ProxyFTableRegion) {
      return (ProxyFTableRegion) table;
    }
    return null;
  }

  @Override
  public Set<Object> getMTableKeySet(String tableName) throws IllegalArgumentException {
    try {

      MTableDescriptor tableDescriptor = (MTableDescriptor) getTableDescriptor(tableName);
      if (tableDescriptor != null) {

        Region r = getRegion(tableName);
        if (r != null) {
          return (r.keySet());
        } else {
          return (new HashSet<Object>());
        }
      }


    } catch (CacheClosedException cce) {
      throw new MCacheInternalErrorException(cce.getMessage());
    }
    return null;
  }

  @Override
  public void close() {
    super.close();
    MonarchCacheImpl.instance = null;
  }

  public void closeAmpoolCache() {
    MonarchCacheImpl.instance = null;
  }

  @Override
  public int getSocketBufferSize() {
    return getDefaultPool().getSocketBufferSize();
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    if (getMyId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
      if (!internalRegionArgs.isUsedForMetaRegion()
          && internalRegionArgs.getInternalMetaRegion() == null) {
        throw new IllegalStateException("Regions can not be created in a locator.");
      }
    }
    super.getCancelCriterion().checkCancelInProgress(null);
    LocalRegion.validateRegionName(name, internalRegionArgs);
    RegionAttributes<K, V> attrs = p_attrs;
    attrs = invokeRegionBefore(null, name, attrs, internalRegionArgs);
    if (attrs == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.GemFireCache_ATTRIBUTES_MUST_NOT_BE_NULL.toLocalizedString());
    }

    LocalRegion rgn = null;
    // final boolean getDestroyLock = attrs.getDestroyLockFlag();
    final InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();
    final boolean recreate = internalRegionArgs.getRecreateFlag();

    final boolean isPartitionedRegion = attrs.getPartitionAttributes() != null;
    final boolean isReinitCreate = snapshotInputStream != null || imageTarget != null || recreate;
    final boolean isAmpoolTable =
        AmpoolTableRegionAttributes.isAmpoolTable(attrs.getCustomAttributes());



    try {
      for (;;) {
        getCancelCriterion().checkCancelInProgress(null);

        Future future = null;
        synchronized (this.rootRegions) {
          rgn = (LocalRegion) this.rootRegions.get(name);
          if (rgn != null) {
            throw new RegionExistsException(rgn);
          }
          // check for case where a root region is being reinitialized and we
          // didn't
          // find a region, i.e. the new region is about to be created

          if (!isReinitCreate) { // fix bug 33523
            String fullPath = Region.SEPARATOR + name;
            future = (Future) this.reinitializingRegions.get(fullPath);
          }
          if (future == null) {
            if (isAmpoolTable) {
              if (AmpoolTableRegionAttributes.isAmpoolMTable(attrs.getCustomAttributes())) {
                // create MTable
                if (attrs.getScope().isDistributedAck())
                  rgn = new TableDistributedRegion(name, attrs, null, this, internalRegionArgs);
                if (attrs.getScope().isDistributedNoAck())
                  rgn = new MTableRegion(name, attrs, null, this, internalRegionArgs);
              } else if (AmpoolTableRegionAttributes.isAmpoolFTable(attrs.getCustomAttributes())) {
                // create FTable
                rgn = new FTableRegion(name, attrs, null, this, internalRegionArgs);
              }
            } else {
              if (internalRegionArgs.getInternalMetaRegion() != null) {
                rgn = internalRegionArgs.getInternalMetaRegion();
              } else if (isPartitionedRegion) {
                rgn = new PartitionedRegion(name, attrs, null, this, internalRegionArgs);
              } else {
                if (attrs.getScope().isLocal()) {
                  rgn = new TableLocalRegion(name, attrs, null, this, internalRegionArgs);
                } else {
                  rgn = new TableDistributedRegion(name, attrs, null, this, internalRegionArgs);
                }
              }
            }
            this.rootRegions.put(name, rgn);
            if (isReinitCreate) {
              regionReinitialized(rgn);
            }
            break;
          }
        } // synchronized

        boolean interrupted = Thread.interrupted();
        try { // future != null
          LocalRegion region = (LocalRegion) future.get(); // wait on Future
          throw new RegionExistsException(region);
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          throw new Error(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString(),
              e);
        } catch (CancellationException e) {
          // future was cancelled
        } finally {
          if (interrupted)
            Thread.currentThread().interrupt();
        }
      } // for

      boolean success = false;
      try {
        setRegionByPath(rgn.getFullPath(), rgn);
        rgn.initialize(snapshotInputStream, imageTarget, internalRegionArgs);
        success = true;
      } catch (CancelException e) {
        // don't print a call stack
        throw e;
      } catch (RedundancyAlreadyMetException e) {
        // don't log this
        throw e;
      } catch (final RuntimeException validationException) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GemFireCache_INITIALIZATION_FAILED_FOR_REGION_0, rgn.getFullPath()),
            validationException);
        throw validationException;
      } finally {
        if (!success) {
          try {
            // do this before removing the region from
            // the root set to fix bug 41982.
            rgn.cleanupFailedInitialization();
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            SystemFailure.checkFailure();
            super.getCancelCriterion().checkCancelInProgress(t);

            // bug #44672 - log the failure but don't override the original exception
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.GemFireCache_INIT_CLEANUP_FAILED_FOR_REGION_0, rgn.getFullPath()),
                t);

          } finally {
            // clean up if initialize fails for any reason
            setRegionByPath(rgn.getFullPath(), null);
            synchronized (this.rootRegions) {
              Region r = (Region) this.rootRegions.get(name);
              if (r == rgn) {
                this.rootRegions.remove(name);
              }
            } // synchronized
          }
        } // success
      }



      rgn.postCreateRegion();
    } catch (RegionExistsException ex) {
      // outside of sync make sure region is initialized to fix bug 37563
      LocalRegion r = (LocalRegion) ex.getRegion();
      r.waitOnInitialization(); // don't give out ref until initialized
      throw ex;
    }

    invokeRegionAfter(rgn);
    /**
     * Added for M&M . Putting the callback here to avoid creating RegionMBean in case of Exception
     **/
    if (!rgn.isInternalRegion()) {
      super.getSystem().handleResourceEvent(ResourceEvent.REGION_CREATE, rgn);
    }

    return rgn;
  }

  private static void createTierStores(Region storeMetaRegion, MonarchCacheImpl monarchCacheImpl)
      throws IllegalAccessException, InstantiationException, DefaultConstructorMissingException,
      StoreCreateException, InvocationTargetException, ClassNotFoundException {
    boolean isDefaultStoreAva = false;

    Iterator iterator = storeMetaRegion.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Map<String, Object>> record =
          (Map.Entry<String, Map<String, Object>>) iterator.next();
      String storeName = record.getKey();
      if (storeName.equals(DefaultStore.STORE_NAME)) {
        isDefaultStoreAva = true;
      }
      Map<String, Object> properties = record.getValue();
      try {
        TierStore tierStore = new TierStoreFactory().create(
            (String) properties.get(TierStoreFactory.STORE_HANDLER_PROP), storeName,
            (Properties) properties.get(TierStoreFactory.STORE_OPTS_PROP),
            new TierStoreWriterFactory().create(
                (String) properties.get(TierStoreFactory.STORE_WRITER_PROP),
                (Properties) properties.get(TierStoreFactory.STORE_WRITER_PROPS_PROP)),
            new TierStoreReaderFactory().create(
                (String) properties.get(TierStoreFactory.STORE_READER_PROP),
                (Properties) properties.get(TierStoreFactory.STORE_READER_PROPS_PROP)),
            monarchCacheImpl);
        StoreHandler.getInstance().registerStore(storeName, tierStore);
      } catch (ClassNotFoundException | DefaultConstructorMissingException | StoreCreateException
          | IllegalAccessException | InvocationTargetException | InstantiationException e) {
        logger.error("Failed to create store: " + storeName, e);
        throw e;
      } catch (Throwable t) {
        logger.error("Failed to create store: " + storeName, t);
        throw t;
      }
    }
    if (!isDefaultStoreAva) {
      // create default store and register in meta region
      boolean tierStore = false;
      try {
        TierStoreReader reader = new TierStoreReaderFactory().create(DefaultStore.ORC_READER_CLASS,
            DefaultStore.getDefaultStoreORCReaderProperties());
        TierStoreWriter writer = new TierStoreWriterFactory().create(DefaultStore.ORC_WRITER_CLASS,
            DefaultStore.getDefaultStoreORCWriterProperties());
        TierStore store =
            new TierStoreFactory().create(DefaultStore.STORE_CLASS, DefaultStore.STORE_NAME,
                DefaultStore.getDefaultStoreProperties(), writer, reader, monarchCacheImpl);
        StoreHandler.getInstance().registerStore(DefaultStore.STORE_NAME, store);
        Map<String, Object> storePropsMap = new HashMap<>();
        storePropsMap.put(TierStoreFactory.STORE_NAME_PROP, DefaultStore.STORE_NAME);
        storePropsMap.put(TierStoreFactory.STORE_HANDLER_PROP, DefaultStore.STORE_CLASS);
        storePropsMap.put(TierStoreFactory.STORE_OPTS_PROP,
            DefaultStore.getDefaultStoreProperties());
        storePropsMap.put(TierStoreFactory.STORE_WRITER_PROP, DefaultStore.ORC_WRITER_CLASS);
        storePropsMap.put(TierStoreFactory.STORE_WRITER_PROPS_PROP,
            DefaultStore.getDefaultStoreORCWriterProperties());
        storePropsMap.put(TierStoreFactory.STORE_READER_PROP, DefaultStore.ORC_READER_CLASS);
        storePropsMap.put(TierStoreFactory.STORE_READER_PROPS_PROP,
            DefaultStore.getDefaultStoreORCReaderProperties());
        storeMetaRegion.put(DefaultStore.STORE_NAME, storePropsMap);


      } catch (ClassNotFoundException | DefaultConstructorMissingException | StoreCreateException
          | IllegalAccessException | InvocationTargetException | InstantiationException e) {
        String successMsg = "Failed to create default tier store {0}. " + "Store class: {1}"
            + "Store Properties: {2} " + "Reader class: {3} " + "Reader properties: {4} "
            + "Writer class: {5} " + "Writer properties: {6} ";
        logger
            .error(CliStrings.format(successMsg, DefaultStore.STORE_NAME, DefaultStore.STORE_CLASS,
                DefaultStore.getDefaultStoreProperties().toString(), DefaultStore.ORC_READER_CLASS,
                DefaultStore.getDefaultStoreORCReaderProperties().toString(),
                DefaultStore.ORC_WRITER_CLASS,
                DefaultStore.getDefaultStoreORCWriterProperties().toString()));
        logger.error(e);
        throw new StoreCreateException(e);
      } catch (Throwable e) {
        String successMsg = "Failed to create default tier store {0}. " + "Store class: {1}"
            + "Store Properties: {2} " + "Reader class: {3} " + "Reader properties: {4} "
            + "Writer class: {5} " + "Writer properties: {6} ";
        logger
            .error(CliStrings.format(successMsg, DefaultStore.STORE_NAME, DefaultStore.STORE_CLASS,
                DefaultStore.getDefaultStoreProperties().toString(), DefaultStore.ORC_READER_CLASS,
                DefaultStore.getDefaultStoreORCReaderProperties().toString(),
                DefaultStore.ORC_WRITER_CLASS,
                DefaultStore.getDefaultStoreORCWriterProperties().toString()));
        logger.error(e);
        throw new StoreCreateException(e);
      }

      if (tierStore) {
        String successMsg = "Tier store {0} created successfully. " + "Store class: {1}"
            + "Store Properties: {2} " + "Reader class: {3} " + "Reader properties: {4} "
            + "Writer class: {5} " + "Writer properties: {6} ";
        logger.info(CliStrings.format(successMsg, DefaultStore.STORE_NAME, DefaultStore.STORE_CLASS,
            DefaultStore.getDefaultStoreProperties().toString(), DefaultStore.ORC_READER_CLASS,
            DefaultStore.getDefaultStoreORCReaderProperties().toString(),
            DefaultStore.ORC_WRITER_CLASS,
            DefaultStore.getDefaultStoreORCWriterProperties().toString()));
      }
    }

  }

  private static void createAmpoolTables(Region metaRegion,
      final MonarchCacheImpl monarchCacheImpl) {
    Iterator iterator = metaRegion.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, MTableDescriptor> record =
          (Map.Entry<String, MTableDescriptor>) iterator.next();
      if (record == null) {
        continue;
      }
      String tableName = record.getKey();
      TableDescriptor tableDescriptor = record.getValue();

      // TODO CHeck if cdc info is passed well
      try {
        MTableUtils.createRegionInGeode(monarchCacheImpl, tableName, tableDescriptor);
      } catch (MTableExistsException e) {
        // Just handle exception, it should be fine just log and move ahead
        logger.info("MTable already exists: " + tableName);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public MTableRegionImpl getTableRegion(String name, int targetBucketId) {
    PartitionedRegion region = (PartitionedRegion) getRegion(name);
    if (region == null) {
      throw new IllegalStateException("Table " + name + " Not found");
    }
    if (!region.getDataStore().getAllLocalBucketIds().contains(targetBucketId)) {
      throw new MCacheInternalErrorException("Invalid node found ");
    }
    return new MTableRegionImpl(name, targetBucketId);
  }


  public static MonarchCacheImpl createClient(DistributedSystem system, PoolFactory pf,
      CacheConfig cacheConfig, boolean isMetaRgnCached) {
    isMetaRegionCached = isMetaRgnCached;
    return basicCreate(system, true, cacheConfig, pf, true, ASYNC_EVENT_LISTENERS, null);
  }

  private static MonarchCacheImpl basicCreate(DistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig, PoolFactory pf, boolean isClient, boolean asyncEventListeners,
      TypeRegistry typeRegistry) throws CacheExistsException, TimeoutException,
      CacheWriterException, GatewayException, RegionExistsException {
    try {
      synchronized (MonarchCacheImpl.class) {
        MonarchCacheImpl instance = checkExistingCache(existingOk, cacheConfig);
        if (instance == null) {
          instance = new MonarchCacheImpl(isClient, pf, system, cacheConfig, asyncEventListeners,
              typeRegistry);
          instance.initialize();
          instance.amplInitialize();
        }
        return instance;
      }
    } catch (CacheXmlException | IllegalArgumentException e) {
      logger.error(e.getLocalizedMessage());
      throw e;
    } catch (Error | RuntimeException e) {
      logger.error(e);
      throw e;
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException
        | DefaultConstructorMissingException | StoreCreateException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }


  private void amplInitialize()
      throws IllegalAccessException, InstantiationException, DefaultConstructorMissingException,
      StoreCreateException, InvocationTargetException, ClassNotFoundException {
    setAmpoolSystem();
    initializeAmpoolCommands();
    MonarchCacheImpl.instance = this;
    int vmKind = getDistributedSystem().getDistributedMember().getVmKind();

    // boolean clientCachingProxyMetaRegion = isClient
    // && "true".equals(conf.get(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING));

    Region metaRegion = getRegion(MTableUtils.AMPL_META_REGION_NAME);
    if (metaRegion == null) {
      // create meta region
      metaRegion = MTableUtils.createMetaRegion(this, isMetaRegionCached);
    }

    if (vmKind == DistributionManager.NORMAL_DM_TYPE
        || vmKind == DistributionManager.LONER_DM_TYPE && !isClient()) {

      if (findDiskStore(MTableUtils.DEFAULT_DISK_STORE_NAME) == null) {
        // Create the default disk store for all the tables
        // This will be used only when use did not specify the disk store name.
        DiskStoreFactory diskStoreFactory = createDiskStoreFactory();
        diskStoreFactory.create(MTableUtils.DEFAULT_DISK_STORE_NAME);
      }

      Region storeMetaRegion = getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
      if (storeMetaRegion == null) {
        // create store meta region
        storeMetaRegion = MTableUtils.createStoreMetaRegion(this);
      }
      registerInternalFunctions();
      createTierStores(storeMetaRegion, this);
      createAmpoolTables(metaRegion, this);
    }
    getCacheConfig().setPdxPersistent(true);

    admin = new AdminImpl(this);
  }

  private void initializeAmpoolCommands() {
    // initialize ampool commands
    // Scan message
    CommandInitializer.registerCommand(MessageType.SCAN,
        Collections.singletonMap(Version.GFE_91, ScanCommand.getCommand()));
    // Message to get partition resolver attribute
    // CommandInitializer.registerCommand(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES, Collections
    // .singletonMap(Version.GFE_91, GetClientPartitionAttributesCommand91.getCommand()));
  }

  private static MonarchCacheImpl checkExistingCache(boolean existingOk, CacheConfig cacheConfig) {
    MonarchCacheImpl instance = getInstance();

    if (instance != null && !instance.isClosed()) {
      if (existingOk) {
        // Check if cache configuration matches.
        cacheConfig.validateCacheConfig(instance);
        return instance;
      } else {
        // instance.creationStack argument is for debugging...
        throw new CacheExistsException(instance,
            LocalizedStrings.CacheFactory_0_AN_OPEN_CACHE_ALREADY_EXISTS
                .toLocalizedString(instance),
            instance.creationStack);
      }
    }
    return null;
  }

  public Region createMemoryTable(final String tableName,
      final TableDescriptor inputTableDescriptor) throws MTableExistsException {
    // From here go to
    // One server and start transaction
    // As part of transaction, create region on every server
    // add info to meta region
    // complete the transaction
    // if fail roll back
    // TODO : Remove this
    TableDescriptor tableDescriptor = inputTableDescriptor;
    // boolean isUserTable = tableDescriptor.getUserTable();
    Region tableRegion = null;

    if (getDistributedSystem().isLoner() && !isClient()) {
      final Region<String, TableDescriptor> metaRegion =
          getRegion(MTableUtils.AMPL_META_REGION_NAME);
      final Lock dLock = metaRegion.getDistributedLock(tableName);
      dLock.lock();
      try {
        tableRegion = MTableUtils.createRegionInGeode(this, tableName, inputTableDescriptor);
        if (tableRegion != null) {
          metaRegion.put(tableName, tableDescriptor);
        }
      } catch (Throwable t) {
        throw t;
      } finally {
        dLock.unlock();
      }
    } else {
      Function createMTableControllerFunction = new CreateMTableControllerFunction();
      FunctionService.registerFunction(createMTableControllerFunction);
      List<Object> inputList = new ArrayList<Object>();
      // TODO Changed there shouldn't be a need to pass arguments, tableDescriptor should do.
      inputList.add(tableName);
      inputList.add(tableDescriptor);
      Execution members;
      if (isClient()) {
        members = FunctionService.onServer(getDefaultPool()).withArgs(inputList);
      } else {
        final Iterator<DistributedMember> memberSetItr =
            MTableUtils.getAllDataMembers(this).iterator();
        DistributedMember member = null;
        if (memberSetItr.hasNext()) {
          member = memberSetItr.next();
        }
        members = FunctionService.onMember(member).withArgs(inputList);
      }

      List createTableResult = null;
      Boolean finalRes = true;
      Exception ex = null;
      try {
        createTableResult =
            (ArrayList) members.execute(createMTableControllerFunction.getId()).getResult();
      } catch (Exception e) {
        // If function execution is failed due to member failure
        // then handle it by rollbacking...
        // Function HA should handle this.
        ex = e;
        finalRes = false;
      }

      if (createTableResult != null) {
        for (int i = 0; i < createTableResult.size(); i++) {
          Object receivedObj = createTableResult.get(i);
          if (receivedObj instanceof Boolean) {
            finalRes = finalRes && (Boolean) receivedObj;
          }
          if (receivedObj instanceof Exception) {
            finalRes = finalRes && false;
            ex = (Exception) receivedObj;
          }
        }
      }
      if (!finalRes) {
        logger.error("Creation of Memory Table " + tableName + " failed with exception " + ex);
        if (ex instanceof MTableExistsException) {
          throw (MTableExistsException) ex;
        }
        if (ex instanceof IllegalArgumentException) {
          throw (IllegalArgumentException) ex;
        }
        if (ex instanceof MCacheInternalErrorException) {
          throw (MCacheInternalErrorException) ex;
        }
        if (ex instanceof TierStoreNotAvailableException) {
          throw (TierStoreNotAvailableException) ex;
        }
        if (ex instanceof MCoprocessorException) {
          throw (MCoprocessorException) ex;
        }

        MTableUtils.checkSecurityException(ex);

        if (ex instanceof GemFireException) {
          throw (GemFireException) ex;
        }
        if (ex instanceof RuntimeException) {
          throw (RuntimeException) ex;
        }
      }
      logger.debug("Creation of MTable " + tableName + " successful");
      if (isClient() && tableRegion == null) {
        try {
          ClientRegionFactory<MTableKey, byte[]> crf =
              createClientRegionFactory(ClientRegionShortcut.PROXY);
          tableRegion = crf.create(tableName);
        } catch (RegionExistsException ree) {
          tableRegion = getRegion(tableName);
        }
      } else {
        tableRegion = getRegion(tableName);
      }
      return tableRegion;
    }
    return tableRegion;
    // check with "createregioningeodeFnExec"
  }

  public byte[] getDummyValue(byte[] value, List<Integer> columns, String regionName) {
    MTableDescriptor tableDescriptor = (MTableDescriptor) getTableDescriptor(regionName);
    int numOfColumns = columns.size();

    if (numOfColumns == 0) {
      numOfColumns = tableDescriptor.getAllColumnDescriptors().size();
    }
    // create dummy descriptor with all

    BitMap map = new BitMap(numOfColumns);
    for (int i = 0; i < numOfColumns; i++) {
      map.set(i);
    }
    // write RowHeader + TS + BitMap + Value Size * NumofCols + INT size * NumofCols
    int size = RowHeader.getHeaderLength() + Bytes.SIZEOF_LONG + map.sizeOfByteArray()
        + value.length * numOfColumns + Bytes.SIZEOF_INT * numOfColumns;
    byte[] dummyArr = new byte[size];
    int dataPos = 0;

    System.arraycopy(tableDescriptor.getRowHeaderBytes(), 0, dummyArr, dataPos,
        RowHeader.getHeaderLength());
    dataPos += RowHeader.getHeaderLength();

    // TS
    System.arraycopy(Bytes.toBytes(System.nanoTime()), 0, dummyArr, dataPos, Bytes.SIZEOF_LONG);
    dataPos += Bytes.SIZEOF_LONG;

    // BitMap
    System.arraycopy(map.toByteArray(), 0, dummyArr, dataPos, map.sizeOfByteArray());
    dataPos += map.sizeOfByteArray();

    int offSetPos = dummyArr.length - Bytes.SIZEOF_INT;

    for (int i = 0; i < numOfColumns; i++) {
      // write offset
      System.arraycopy(Bytes.toBytes(dataPos), 0, dummyArr, offSetPos, Bytes.SIZEOF_INT);
      offSetPos -= Bytes.SIZEOF_INT;

      // write value
      System.arraycopy(value, 0, dummyArr, dataPos, value.length);
      dataPos += value.length;
    }
    return dummyArr;
  }

  public static MCache create(DistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig) {
    return basicCreate(system, existingOk, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }

  public static MonarchCacheImpl create(DistributedSystem system, CacheConfig cacheConfig) {
    return basicCreate(system, true, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }


  @Override
  public MCache getReconnectedCache() {
    MonarchCacheImpl c = MonarchCacheImpl.getInstance();
    if (c == null) {
      return null;
    }
    if (c == this || !c.isInitialized()) {
      c = null;
    }
    return c;
  }


  public static MonarchCacheImpl getExisting() {
    final MonarchCacheImpl result = instance;
    if (result != null && !result.isClosing) {
      return result;
    }
    if (result != null) {
      throw result.getCacheClosedException(
          LocalizedStrings.CacheFactory_THE_CACHE_HAS_BEEN_CLOSED.toLocalizedString(), null);
    }
    throw new CacheClosedException(
        LocalizedStrings.CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED.toLocalizedString());
  }

  @Deprecated
  public static GemFireCacheImpl getGeodeCacheInstance() {
    return instance;
  }
}
