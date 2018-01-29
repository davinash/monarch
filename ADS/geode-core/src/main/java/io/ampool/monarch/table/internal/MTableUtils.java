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
import io.ampool.conf.Constants;
import io.ampool.internal.AmpoolOpType;
import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MAsyncEventListener;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAction;
import io.ampool.monarch.table.MExpirationAttributes;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorManager;
import io.ampool.monarch.table.coprocessor.internal.PostOpRegionObserver;
import io.ampool.monarch.table.coprocessor.internal.PreOpRegionObserver;
import io.ampool.monarch.table.exceptions.MCacheClosedException;
import io.ampool.monarch.table.exceptions.MCacheInvalidConfigurationException;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.PartitionResolver;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.TierStore;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.*;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.logging.log4j.Logger;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.ampool.conf.Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING;
import static io.ampool.conf.Constants.MClientCacheconfig.ENABLE_POOL_SUBSCRIPTION;
import static io.ampool.conf.Constants.MClientCacheconfig.MONARCH_CLIENT_LOG;
import static io.ampool.conf.Constants.MClientCacheconfig.MONARCH_CLIENT_READ_TIMEOUT;
import static io.ampool.conf.Constants.MClientCacheconfig.MONARCH_CLIENT_SOCK_BUFFER_SIZE;
import static io.ampool.conf.Constants.MonarchLocator.MONARCH_LOCATORS;
import static io.ampool.conf.Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS;
import static io.ampool.conf.Constants.MonarchLocator.MONARCH_LOCATOR_PORT;
import static io.ampool.monarch.table.internal.MTableStorageFormatter.BITMAP_START_POS;


/**
 * Misc class to have common/repeated code at one place. Started with handling of MetaTable Related
 * Code
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableUtils {

  private static final Logger logger = LogService.getLogger();
  public static final String DEFAULT_MTABLE_DISK_STORE_NAME = "AMPOOL_DEFAULT_MTABLE_DISK_STORE";
  public static final String DEFAULT_FTABLE_DISK_STORE_NAME = "AMPOOL_DEFAULT_FTABLE_DISK_STORE";
  public static final String KEY_COLUMN_NAME = "__ROW__KEY__COLUMN__";
  public static final byte[] KEY_COLUMN_NAME_BYTES = KEY_COLUMN_NAME.getBytes();
  public static final int ONE_MEGA_BYTE = 1_048_576;

  public static final String AMPL_META_REGION_NAME = ".AMPOOL.MONARCH.TABLE.META.";
  public static final String AMPL_STORE_META_REGION_NAME = ".AMPOOL.MONARCH.STORES.META.";

  public static final String AMPL_DELTA_PERS_PROP_NAME = "ampool.ftableDeltaPersistence";



  public static String getLocatorAddress(MConfiguration conf) {
    try {
      return conf.get(MONARCH_LOCATOR_ADDRESS);
    } catch (NullPointerException npe) {
      throw new RuntimeException(npe);
    }
  }

  public static boolean isAmpoolProperty(final String propertyName) {
    if (MONARCH_LOCATOR_ADDRESS.equals(propertyName) || MONARCH_LOCATOR_PORT.equals(propertyName)
        || MONARCH_LOCATORS.equals(propertyName) || MONARCH_CLIENT_LOG.equals(propertyName)
        || ENABLE_META_REGION_CACHING.equals(propertyName)
        || ENABLE_POOL_SUBSCRIPTION.equals(propertyName)
        || MONARCH_CLIENT_SOCK_BUFFER_SIZE.equals(propertyName)
        || MONARCH_CLIENT_READ_TIMEOUT.equals(propertyName)) {
      return true;
    }
    return false;
  }

  public static List<Pair<String, Integer>> getLocators(MConfiguration conf) {
    try {
      List locatorsList = new ArrayList();
      String locators = conf.get(Constants.MonarchLocator.MONARCH_LOCATORS);
      if (locators != null && locators.length() != 0) {
        String[] locatorSpecs = locators.split(",");
        for (String locatorSpec : locatorSpecs) {
          String[] addrPort = locatorSpec.split(Pattern.quote("["));
          if (addrPort.length != 2) {
            throw new MCacheInvalidConfigurationException(
                "Allowed format for locators is: \"addr[port], addr[port]\"");
          } else if (addrPort[1].charAt(addrPort[1].length() - 1) != ']') {
            throw new MCacheInvalidConfigurationException(
                "Allowed format for locators is: \"addr[port], addr[port]\"");
          } else if (addrPort[1].length() < 2) {
            throw new MCacheInvalidConfigurationException(
                "Allowed format for locators is: \"addr[port], addr[port]\"");
          }
          String addr = addrPort[0];
          Integer port = Integer.parseInt(addrPort[1].substring(0, addrPort[1].length() - 1));
          locatorsList.add(new Pair<>(addr, port));
        }
      }
      return locatorsList;
    } catch (NullPointerException npe) {
      throw new RuntimeException(npe);
    }
  }

  public static int getLocatorPort(MConfiguration conf) {
    try {
      return conf.getInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, 0);
    } catch (NullPointerException npe) {
      throw new RuntimeException(npe);
    }

  }

  public static int getClientSocketBufferSize(MConfiguration conf) {
    return conf.getInt(MONARCH_CLIENT_SOCK_BUFFER_SIZE, 32 * 1024);
  }

  public static int getClientPoolReadTimeOut(MConfiguration conf) {
    return conf.getInt(MONARCH_CLIENT_READ_TIMEOUT, 10000);
  }

  public static Set<DistributedMember> getAllDataMembers(MCache cache) {
    Set<DistributedMember> normalDataMembers = new HashSet<>();
    Set<DistributedMember> allPeers = cache.getDistributedSystem().getAllOtherMembers();
    allPeers.add(cache.getDistributedSystem().getDistributedMember());

    allPeers.forEach((DM) -> {
      if (((InternalDistributedMember) DM).getVmKind() == DistributionManager.NORMAL_DM_TYPE) {
        normalDataMembers.add(DM);
      }
    });
    return normalDataMembers;
  }

  private static void executeRegionCreateFnOnServer(MonarchCacheImpl cache,
      final String regionName) {
    try {
      Function atcf = new MTableCreationFunction();
      FunctionService.registerFunction(atcf);

      MCreateFunctionArguments funArgs =
          new MCreateFunctionArguments(regionName, false, null, 0, 0, null, null, false, null, null,
              null, MDiskWritePolicy.ASYNCHRONOUS, MEvictionPolicy.OVERFLOW_TO_DISK, null, null);

      java.util.List<Object> inputList = new java.util.ArrayList<Object>();
      inputList.add(funArgs);

      Execution members;
      if (cache.isClient()) {
        members = FunctionService.onServers(cache.getDefaultPool()).withArgs(inputList);
      } else {
        members = FunctionService.onMembers(getAllDataMembers(cache)).withArgs(inputList);
      }

      members.execute(atcf.getId()).getResult();
    } catch (Exception e) {
      if (e instanceof FunctionException) {
        if (e.getCause().getClass() == NullPointerException.class) {
          if (!cache.isClosed()) {
            cache.close();
          }
          throw new MCacheClosedException("Could not make connection with Locator/Server");
        }
      }
    }

  }

  /**
   * Creates a meta-region This region is used to store the meta information about the table. See
   * {@link MTableDescriptor} for meta information.
   *
   * @param cache the Geode cache
   * @param clientCachingProxy true if caching-proxy to be used for client; false otherwise
   */
  public static Region<String, TableDescriptor> createMetaRegion(MonarchCacheImpl cache,
      boolean clientCachingProxy) {
    Region<String, TableDescriptor> metaRegion = cache.getRegion(AMPL_META_REGION_NAME);
    if (metaRegion == null) {
      if (cache.isClient()) {
        final ClientRegionFactory<String, TableDescriptor> clientRegionFactory =
            cache.createClientRegionFactory(clientCachingProxy ? ClientRegionShortcut.CACHING_PROXY
                : ClientRegionShortcut.PROXY);
        metaRegion = clientRegionFactory.create(AMPL_META_REGION_NAME);
      } else if (cache.getDistributedSystem().getDistributedMember()
          .getVmKind() == DistributionManager.NORMAL_DM_TYPE
          || cache.getDistributedSystem().getDistributedMember()
              .getVmKind() == DistributionManager.LONER_DM_TYPE) {
        AttributesFactory<String, TableDescriptor> regionAttrsFactory =
            new AttributesFactory<String, TableDescriptor>();
        regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionAttrsFactory.setScope(Scope.GLOBAL);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        try {
          metaRegion = cache.createVMRegion(AMPL_META_REGION_NAME, regionAttrsFactory.create(),
              internalArgs);
        } catch (IOException e) {
          logger.error("Metaregion creation failed." + e);
        } catch (ClassNotFoundException e) {
          logger.error("Metaregion creation failed." + e);
        }
        // test hook
        if (META_REGION_SNAPSHOT != null) {
          try {
            metaRegion.getSnapshotService().load(new File(getMetaRegionSnapshot()),
                SnapshotOptions.SnapshotFormat.GEMFIRE);
          } catch (Exception ex) {
            // ex.printStackTrace();
          }
        }
      }
    }
    return metaRegion;
  }


  /**
   * Creates the store meta-region This region is used to store the meta information about the tier
   * stores.
   */
  public static Region<String, Map<String, Object>> createStoreMetaRegion(MonarchCacheImpl cache) {
    Region<String, Map<String, Object>> storeMetaRegion =
        cache.getRegion(AMPL_STORE_META_REGION_NAME);
    if (storeMetaRegion == null) {
      if (cache.isClient()) {
        final ClientRegionFactory<String, Map<String, Object>> clientRegionFactory =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        storeMetaRegion = clientRegionFactory.create(AMPL_STORE_META_REGION_NAME);
      } else if (cache.getDistributedSystem().getDistributedMember()
          .getVmKind() == DistributionManager.NORMAL_DM_TYPE
          || cache.getDistributedSystem().getDistributedMember()
              .getVmKind() == DistributionManager.LONER_DM_TYPE) {
        AttributesFactory<String, Map<String, Object>> regionAttrsFactory =
            new AttributesFactory<String, Map<String, Object>>();
        regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionAttrsFactory.setScope(Scope.GLOBAL);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        try {
          storeMetaRegion = cache.createVMRegion(AMPL_STORE_META_REGION_NAME,
              regionAttrsFactory.create(), internalArgs);
        } catch (IOException e) {
          logger.error("Store metaregion creation failed." + e);
        } catch (ClassNotFoundException e) {
          logger.error("Store metaregion creation failed." + e);
        }
      }
    }
    return storeMetaRegion;
  }



  /**
   * Valid namespace characters are [a-zA-Z_0-9]
   *
   * @param tableName table name
   */
  public static void isTableNameLegal(final String tableName) {
    if (tableName == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    int end = tableName.length();
    int start = 0;
    if (end - start < 1) {
      throw new IllegalArgumentException("TableName must not be empty");
    }
    if (tableName.indexOf("/") >= 0) {
      throw new IllegalArgumentException("TableName must not contain '/' ");
    }
    if (tableName.startsWith("__")) {
      throw new IllegalArgumentException("Table name should not begin with a double-underscore");
    }

    for (int i = start; i < end; i++) {
      if (Character.isLetterOrDigit(tableName.charAt(i)) || tableName.charAt(i) == '_'
          || tableName.charAt(i) == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + tableName.charAt(i) + "> at " + i
          + ". Name can only contain " + "'alphanumeric characters': i.e. [a-zA-Z_0-9].");
    }
  }


  public static void checkSecurityException(Exception ex) {
    if (ex instanceof AuthenticationRequiredException) {
      throw (AuthenticationRequiredException) ex;
    }
    if (ex.getCause() instanceof AuthenticationRequiredException) {
      throw (AuthenticationRequiredException) ex.getCause();
    }
    if (ex.getCause() != null && ex.getCause().getCause() instanceof NotAuthorizedException) {
      throw (NotAuthorizedException) ex.getCause().getCause();
    }

    if (ex instanceof AuthenticationFailedException) {
      throw (AuthenticationFailedException) ex;
    }
    if (ex.getCause() instanceof AuthenticationFailedException) {
      throw (AuthenticationFailedException) ex.getCause();
    }
    if (ex.getCause() != null
        && ex.getCause().getCause() instanceof AuthenticationFailedException) {
      throw (AuthenticationFailedException) ex.getCause().getCause();
    }

    if (ex instanceof NotAuthorizedException) {
      throw (NotAuthorizedException) ex;
    }
    if (ex.getCause() instanceof NotAuthorizedException) {
      throw (NotAuthorizedException) ex.getCause();
    }
    if (ex.getCause() != null && ex.getCause().getCause() instanceof NotAuthorizedException) {
      throw (NotAuthorizedException) ex.getCause().getCause();
    }
  }

  // public static List<byte[]> getColumns(final String tableName, final MTableKey key) {
  // List<byte[]> columns = new ArrayList<>();
  // final List<Integer> columnPositions = ((MTableKey) key).getColumnPositions();
  // MTableDescriptor tableDescriptor = MTableUtils.getMTableDescriptor(tableName, false);
  // for (Map.Entry<MColumnDescriptor, Integer> mColumnDescriptor : tableDescriptor
  // .getColumnDescriptorsMap().entrySet()) {
  // if (columnPositions.contains(mColumnDescriptor.getValue())) {
  // columns.add(mColumnDescriptor.getKey().getColumnName());
  // }
  // }
  // return columns;
  // }

  public static String getMetaRegionSnapshot() {
    return META_REGION_SNAPSHOT;
  }

  public static void setMetaRegionSnapshot(final String metaRegionSnapshot) {
    META_REGION_SNAPSHOT = metaRegionSnapshot;
  }

  public static void resetMetaRegionSnapshot() {
    META_REGION_SNAPSHOT = null;
  }

  private static String META_REGION_SNAPSHOT = null;


  public static String getTableToBucketIdKey(final String table, final int buckedId) {
    return table + ":" + buckedId;
  }

  /**
   * Returns {@link InetAddress} instance for local host.
   *
   * @return returns InetAddress object for localhost
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    InetAddress localHost;
    localHost = InetAddress.getLocalHost();
    if (localHost.isLoopbackAddress()) {
      InetAddress ipv4Fallback = null;
      InetAddress ipv6Fallback = null;
      // try to find a non-loopback address
      Set myInterfaces = getMyAddresses();
      String lhName = null;
      for (Iterator<InetAddress> it = myInterfaces.iterator(); lhName == null && it.hasNext();) {
        InetAddress addr = it.next();
        if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()) {
          break;
        }
        boolean ipv6 = addr instanceof Inet6Address;
        boolean ipv4 = addr instanceof Inet4Address;
        if (ipv6 || ipv4) {
          String addrName = reverseDNS(addr);
          if (localHost.isLoopbackAddress()) {
            localHost = addr;
            lhName = addrName;
          } else if (addrName != null) {
            localHost = addr;
            lhName = addrName;
          }
        } else {
          if (ipv4 && ipv4Fallback == null) {
            ipv4Fallback = addr;
          } else if (ipv6 && ipv6Fallback == null) {
            ipv6Fallback = addr;
          }
        }
      }
      // vanilla Ubuntu installations will have a usable IPv6 address when
      // running as a guest OS on an IPv6-enabled machine. We also look for
      // the alternative IPv4 configuration.
      if (localHost.isLoopbackAddress()) {
        if (ipv4Fallback != null) {
          localHost = ipv4Fallback;
        } else if (ipv6Fallback != null) {
          localHost = ipv6Fallback;
        }
      }
    }
    return localHost;
  }

  /**
   * returns a set of the non-loopback InetAddresses for this machine
   */
  private static Set<InetAddress> getMyAddresses() {
    Set<InetAddress> result = new HashSet<InetAddress>();
    Set<InetAddress> locals = new HashSet<InetAddress>();
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new IllegalArgumentException("Unable to examine network interfaces", e);
    }
    while (interfaces.hasMoreElements()) {
      NetworkInterface face = interfaces.nextElement();
      boolean faceIsUp = false;
      try {
        faceIsUp = face.isUp();
      } catch (SocketException e) {
        logger.info("Failed to check if network interface is up. Skipping {}", face, e);
      }
      if (faceIsUp) {
        Enumeration<InetAddress> addrs = face.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress() || (addr.isLinkLocalAddress())) {
            locals.add(addr);
          } else {
            result.add(addr);
          }
        } // while
      }
    } // while
    // fix for bug #42427 - allow product to run on a standalone box by using
    // local addresses if there are no non-local addresses available
    if (result.size() == 0) {
      return locals;
    } else {
      return result;
    }
  }

  /**
   * This method uses JNDI to look up an address in DNS and return its name
   *
   * @return the host name associated with the address or null if lookup isn't possible or there is
   *         no host name for this address
   */
  private static String reverseDNS(InetAddress addr) {
    byte[] addrBytes = addr.getAddress();
    // reverse the address suitable for reverse lookup

    StringBuilder sb = new StringBuilder();
    for (int index = addrBytes.length - 1; index >= 0; index--) {
      // lookup = lookup + (addrBytes[index] & 0xff) + '.';
      sb.append((addrBytes[index] & 0xff)).append('.');
    }
    sb.append("in-addr.arpa");
    String lookup = sb.toString();

    try {
      Hashtable env = new Hashtable();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      DirContext ctx = new InitialDirContext(env);
      Attributes attrs = ctx.getAttributes(lookup, new String[] {"PTR"});
      for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements();) {
        Attribute attr = (Attribute) ae.next();
        for (Enumeration vals = attr.getAll(); vals.hasMoreElements();) {
          Object elem = vals.nextElement();
          if ("PTR".equals(attr.getID()) && elem != null) {
            return elem.toString();
          }
        }
      }
      ctx.close();
    } catch (Exception e) {
      // ignored
    }
    return null;
  }

  /**
   * Get Buckedid for the given rowkey
   *
   * @return bucketid of the given rowkey
   */
  public static int getBucketId(byte[] row, Map<Integer, Pair<byte[], byte[]>> splitsMap) {
    int bucketId = -1;
    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : splitsMap.entrySet()) {
      Pair<byte[], byte[]> range = entry.getValue();

      byte[] startKey = range.getFirst();
      byte[] stopKey = range.getSecond();
      if ((Bytes.compareToPrefix(row, startKey) >= 0)
          && (Bytes.compareToPrefix(row, stopKey)) <= 0) {
        bucketId = entry.getKey();
        break;
      }
    }
    return bucketId;
  }

  public static int getBucketId(byte[] row, MTableDescriptor tableDescriptor) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap = tableDescriptor.getKeySpace();
    return getBucketId(row, splitsMap);
  }

  /**
   * Get set of bucketIds given start and stop key
   *
   * @param startkey startKey
   * @param endKey endKey
   * @param tableDescriptor tableDescriptor
   * @return returns set of bucketIds given start and stop key
   */
  public static Set<Integer> getBucketIdSet(byte[] startkey, byte[] endKey,
      TableDescriptor tableDescriptor) throws IllegalArgumentException {
    if (tableDescriptor == null) {
      throw new IllegalArgumentException(
          "tableDescriptor cannot be null while invoking getBucketIdSet()!");
    }

    int totalBuckets = tableDescriptor.getTotalNumOfSplits();
    Set<Integer> bucketIdSet = new TreeSet<>();
    if (startkey == null && endKey == null) {
      for (int i = 0; i < totalBuckets; i++) {
        bucketIdSet.add(i);
      }
    } else if (startkey == null && endKey != null) {
      int endBucketId = getEndBucketId(endKey, ((MTableDescriptor) tableDescriptor).getKeySpace());
      for (int i = 0; i <= endBucketId; i++) {
        bucketIdSet.add(i);
      }

    } else if (startkey != null && endKey == null) {
      int startBucketId =
          getStartBucketId(startkey, ((MTableDescriptor) tableDescriptor).getKeySpace());
      for (int i = startBucketId; i < totalBuckets; i++) {
        bucketIdSet.add(i);
      }
    } else {
      if (Bytes.compareTo(endKey, startkey) < 0) {
        throw new IllegalArgumentException("stopkey can not be smaller than startkey!");
      }

      int startBucketId =
          getStartBucketId(startkey, ((MTableDescriptor) tableDescriptor).getKeySpace());
      int endBucketId = getEndBucketId(endKey, ((MTableDescriptor) tableDescriptor).getKeySpace());

      if (startBucketId != -1 && endBucketId != -1) {
        for (int i = startBucketId; i <= endBucketId; i++) {
          bucketIdSet.add(i);
        }
      }
    }
    return bucketIdSet;
  }

  public static int getStartBucketId(byte[] rowKey, Map<Integer, Pair<byte[], byte[]>> keySpace) {
    int buckedId = -1;
    for (int i = 0; i < keySpace.size(); i++) {
      Pair<byte[], byte[]> startEndPair = keySpace.get(i);
      byte[] rangeEndKey = startEndPair.getSecond();
      // Is current rangeEndkey greater or equal than rowkey?
      if (Bytes.compareToPrefix(rangeEndKey, rowKey) >= 0) {
        buckedId = i;
        break;
      }
    }
    return buckedId;
  }

  public static int getEndBucketId(byte[] rowKey, Map<Integer, Pair<byte[], byte[]>> keySpace) {
    int buckedId = -1;
    for (int i = 0; i < keySpace.size(); i++) {
      Pair<byte[], byte[]> startEndPair = keySpace.get(i);
      byte[] rangeStartKey = startEndPair.getFirst();
      byte[] rangeEndKey = startEndPair.getSecond();

      // Is current rangeEndkey greater or equal than rowkey?
      if (Bytes.compareToPrefix(rangeEndKey, rowKey) >= 0) {
        // Is rowkey within this range?
        if (Bytes.compareToPrefix(rangeStartKey, rowKey) <= 0) {
          buckedId = i;
          return buckedId;
        } else {
          // Else it should end in previous range, so return i-1
          buckedId = i - 1;
          return buckedId;
        }
      }
    }
    buckedId = keySpace.size() - 1;
    return buckedId;
  }


  /**
   * Get Start, End key for the given bucketid
   *
   * @return start, end key pair for the input bucketid
   */
  public static Pair<byte[], byte[]> getStartEndKeysOverKeySpace(MTableDescriptor tableDescriptor,
      int bucketId) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap = tableDescriptor.getKeySpace();
    return splitsMap.get(bucketId);
  }

  public static Map<Integer, Pair<byte[], byte[]>> getUniformKeySpaceSplit(
      int totalNumberOfBuckets) {
    return getUniformKeySpaceSplit(totalNumberOfBuckets, null, null);
  }


  /**
   * Code Borrowed from HBase. org.apache.hadoop.hbase.util.RegionSplitter::UniformSplit Probably in
   * future we should customize the Split algorithm for user to define the custom Splitting of the
   * code.
   */
  public static Map<Integer, Pair<byte[], byte[]>> getUniformKeySpaceSplit(int totalNumberOfBuckets,
      byte[] startRangeKey, byte[] stopRangeKey) {
    byte xFF = (byte) 0xFF;
    byte[] EMPTY_BYTE_ARRAY = new byte[0];
    byte[] MAX_BYTE_ARRAY = new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};
    byte[] firstRowBytes;
    byte[] lastRowBytes;

    Map<Integer, Pair<byte[], byte[]>> splitsMap = new HashMap<>();

    if (startRangeKey != null) {
      firstRowBytes = startRangeKey;
    } else {
      firstRowBytes = EMPTY_BYTE_ARRAY;
    }

    if (stopRangeKey != null) {
      lastRowBytes = stopRangeKey;
    } else {
      lastRowBytes = MAX_BYTE_ARRAY;
    }

    if (Bytes.compareTo(firstRowBytes, lastRowBytes) >= 0) {
      throw new IllegalArgumentException("start of range must be <= end of range");
    }

    /**
     * Special case , this should not happen in real world production scenarios. Added here for
     * DUNIT Handling.
     */
    if (totalNumberOfBuckets == 1) {
      splitsMap.put(0, new Pair<>(firstRowBytes, lastRowBytes));
    } else {
      byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes, true, totalNumberOfBuckets - 1);

      if (splits == null) {
        throw new IllegalArgumentException(
            "unable to generate region split with provided parameters");
      }

      for (int index = 1; index < splits.length; index++) {
        byte[] firstByteArr = splits[index - 1];
        /*
         * Example:-
         *
         * Keys recevied from above function are like [10,20,30,40....]
         *
         * We will make pairs as bucket 0 - 0 to 10 bucket 1 - 11 to 20 bucket 2 - 21 to 30 ... ..
         *
         */
        if (index != 1) {
          BigInteger startBI = new BigInteger(splits[index - 1]);
          startBI = startBI.add(BigInteger.ONE);
          firstByteArr = startBI.toByteArray();
        }
        splitsMap.put(index - 1, new Pair<>(firstByteArr, splits[index]));
      }
    }
    return splitsMap;
  }


  public static Set<ServerLocation> getServerLocationForBucketId(MTable table, int bucketId) {
    Map<Integer, Set<ServerLocation>> bucketToServerMap =
        getBucketToServerMap(table.getName(), null, AmpoolOpType.ANY_OP);
    return bucketToServerMap.get(bucketId);
  }

  public static ServerLocation getPrimaryServerLocationForBucketId(MTable table, int bucketId) {
    Map<Integer, ServerLocation> bucketToServerMap =
        getPrimaryBucketToServerMap(table.getName(), null);
    return bucketToServerMap.get(bucketId);
  }

  /**
   * Get the internal table, either MTable or FTable
   *
   * @param tableName the table name
   * @return the internal table
   */
  private static InternalTable getInternalTable(final String tableName) {
    return (InternalTable) ((MonarchCacheImpl) MClientCacheFactory.getAnyInstance())
        .getAnyTable(tableName);
  }

  /**
   * Provide bucket to server-location mapping.
   *
   * @param tableName the table name
   * @param maxBucketCount the number of buckets in the table
   * @return the list of splits
   */
  public static List<MSplit> getSplits(final String tableName, final int numSplits,
      final int maxBucketCount, Map<Integer, Set<ServerLocation>> bucketLocationMap) {
    if (numSplits > maxBucketCount) {
      throw new IllegalArgumentException("Number of splits cannot be greater than number of "
          + "buckets. [numSplits=" + numSplits + ",numBuckets=" + maxBucketCount + "]");
    }
    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    MTableUtils.getLocationMap(getInternalTable(tableName), null, primaryBucketMap, null,
        AmpoolOpType.ANY_OP);

    /**
     * group the buckets by server-location so that co-located buckets can be part of same split.
     */
    Map<ServerLocation, List<Integer>> serverToBuckets;
    serverToBuckets =
        primaryBucketMap.entrySet().stream().filter(e -> e.getValue() != null).map(e -> {
          Set<ServerLocation> set = new LinkedHashSet<>();
          set.add(e.getValue());
          bucketLocationMap.put(e.getKey(), set);
          return e;
        }).collect(Collectors.groupingBy(Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    int bucketsPerSplit = (int) Math.ceil(1.0 * bucketLocationMap.size() / numSplits);
    List<MSplit> splits = new ArrayList<>(numSplits);

    /** in case more number of servers are more.. then provide those many splits **/
    if (numSplits <= serverToBuckets.size()) {
      int index = 0;
      MSplit split;
      for (Map.Entry<ServerLocation, List<Integer>> entry : serverToBuckets.entrySet()) {
        split = new MSplit(index++);
        split.getBuckets().addAll(entry.getValue());
        split.getServers().add(entry.getKey().getHostName());
        splits.add(split);
      }
    } else {
      /**
       * else try to create equal sized splits.. simple logic to create splits.. may be improved
       * later for same number of buckets
       */
      int index = 0;
      MSplit split = null;
      for (Map.Entry<ServerLocation, List<Integer>> entry : serverToBuckets.entrySet()) {
        if (split == null) {
          split = new MSplit(index++);
        }
        int lastSize = split.getBuckets().size();
        int currentSize = entry.getValue().size();
        if ((lastSize + currentSize) <= bucketsPerSplit) {
          split.getBuckets().addAll(entry.getValue());
          split.getServers().add(entry.getKey().getHostName());
        } else {
          if (split.getBuckets().size() > 0) {
            splits.add(split);
            split = new MSplit(index++);
          }
          List<Integer> list = entry.getValue();
          for (int i = 0; i < list.size(); i += bucketsPerSplit) {
            int end = Math.min(i + bucketsPerSplit, list.size());
            split.getBuckets().addAll(list.subList(i, end));
            split.getServers().add(entry.getKey().getHostName());
            splits.add(split);
            split = new MSplit(index++);
          }
        }
      }
      if (split != null && split.getBuckets().size() > 0) {
        splits.add(split);
      }
    }

    return splits;
  }

  /**
   * Provide bucket to server-location mapping.
   *
   * @param tableName the table name
   * @param maxBucketCount the number of buckets in the table
   * @return the list of splits
   */
  public static List<MSplit> getSplitsWithSize(final String tableName, int numSplits,
      final int maxBucketCount, Map<Integer, Set<ServerLocation>> bucketLocationMap) {
    if (numSplits > maxBucketCount) {
      numSplits = maxBucketCount;
    }

    Map<ServerLocation, List<Integer>> serverToBuckets;
    Map<Integer, Pair<ServerLocation, Long>> primaryBucketMap = new HashMap<>(113);
    serverToBuckets =
        MTableUtils.getLocationAndSize(getInternalTable(tableName), primaryBucketMap, null);

    primaryBucketMap.entrySet().forEach(
        e -> bucketLocationMap.put(e.getKey(), Collections.singleton(e.getValue().getFirst())));

    int bucketsPerSplit = (int) Math.ceil(1.0 * bucketLocationMap.size() / numSplits);
    List<MSplit> splits = new ArrayList<>(numSplits);

    /** in case more number of servers are more.. then provide those many splits **/
    if (numSplits <= serverToBuckets.size()) {
      int index = 0;
      MSplit split;
      for (Map.Entry<ServerLocation, List<Integer>> entry : serverToBuckets.entrySet()) {
        split = new MSplit(index++);
        split.getBuckets().addAll(entry.getValue());
        split.getServers().add(entry.getKey().getHostName());
        for (Integer bid : entry.getValue()) {
          split.incrementSize(primaryBucketMap.get(bid).getSecond());
        }
        splits.add(split);
      }
    } else {
      /**
       * else try to create equal sized splits.. simple logic to create splits.. may be improved
       * later for same number of buckets
       */
      int index = 0;
      MSplit split = null;
      for (Map.Entry<ServerLocation, List<Integer>> entry : serverToBuckets.entrySet()) {
        if (split == null) {
          split = new MSplit(index++);
        }
        int lastSize = split.getBuckets().size();
        int currentSize = entry.getValue().size();
        if ((lastSize + currentSize) <= bucketsPerSplit) {
          split.getBuckets().addAll(entry.getValue());
          split.getServers().add(entry.getKey().getHostName());
          for (Integer bid : entry.getValue()) {
            split.incrementSize(primaryBucketMap.get(bid).getSecond());
          }
        } else {
          if (split.getBuckets().size() > 0) {
            splits.add(split);
            split = new MSplit(index++);
          }
          List<Integer> list = entry.getValue();
          for (int i = 0; i < list.size(); i += bucketsPerSplit) {
            int end = Math.min(i + bucketsPerSplit, list.size());
            split.getBuckets().addAll(list.subList(i, end));
            for (Integer bid : list.subList(i, end)) {
              split.incrementSize(primaryBucketMap.get(bid).getSecond());
            }
            split.getServers().add(entry.getKey().getHostName());
            splits.add(split);
            split = new MSplit(index++);
          }
        }
      }
      if (split != null && split.getBuckets().size() > 0) {
        splits.add(split);
      }
    }

    return splits;
  }


  public static List<String> getTierStores() {
    // execute function on member and get the tier stores
    return null;
  }

  public static Map<String, Object> getStoreInfo(String storeName) {
    Map<String, Object> storeInfoMap = (Map<String, Object>) MCacheFactory.getAnyInstance()
        .getRegion(AMPL_STORE_META_REGION_NAME).get(storeName);
    if (storeInfoMap == null) {
      throw new TierStoreNotAvailableException("Tier store " + storeName + " not found");
    }
    return storeInfoMap;
  }

  public static TableDescriptor getTableDescriptor(final MonarchCacheImpl gemfireCache,
      final String tableName) {
    try {
      return (TableDescriptor) gemfireCache.getRegion(AMPL_META_REGION_NAME).get(tableName);
    } catch (GemFireException ex) {
      checkSecurityException(ex);
      throw ex;
    }
  }


  /**
   * The class to hold basic information for split: the index, buckets and servers..
   */
  public static final class MSplit {

    private int index;
    private final Set<Integer> buckets;
    private final Set<String> servers;

    public long getSize() {
      return size;
    }

    private long size = 0;

    public MSplit(final int index) {
      this.index = index;
      this.buckets = new HashSet<>(50);
      this.servers = new HashSet<>(10);
    }

    @Override
    public String toString() {
      return "[Index= " + index + "; Servers= " + servers + "; Buckets= " + buckets + "]";
    }

    public Set<Integer> getBuckets() {
      return buckets;
    }

    public Set<String> getServers() {
      return this.servers;
    }

    public String[] getServersArray() {
      return servers.toArray(new String[servers.size()]);
    }

    public void incrementSize(final long size) {
      this.size += size;
    }
  }

  public static byte[] getSampleKeyForBucketId(MTableDescriptor tableDescriptor, int bucketId) {
    Map<Integer, Pair<byte[], byte[]>> keySpace = tableDescriptor.getKeySpace();
    return keySpace.get(bucketId).getFirst();
  }

  private static ArrayList<AsyncEventQueue> getAsyncEventArrayList(MonarchCacheImpl cache,
      TableDescriptor tableDescriptor) {

    ArrayList<AsyncEventQueue> result = new ArrayList<>();
    ArrayList<CDCInformation> cdcInformations = new ArrayList<>();
    if (tableDescriptor instanceof MTableDescriptor) {
      cdcInformations = ((MTableDescriptor) tableDescriptor).getCdcInformations();
    }

    for (int i = 0; i < cdcInformations.size(); i++) {
      String queueId = cdcInformations.get(i).getQueueId();
      String listenerClassPath = cdcInformations.get(i).getListenerClassPath();
      CDCConfigImpl config = (CDCConfigImpl) cdcInformations.get(i).getCDCConfig();

      AsyncEventQueue asyncQueue = cache.getAsyncEventQueue(queueId);
      boolean isParallel = true;
      if (asyncQueue != null) {
      } else {
        AsyncEventQueueFactoryImpl factory =
            (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
        factory.setParallel(isParallel);
        if (config != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("MTableUtils.createRegionInGeode config.getBatchSize() = "
                + config.getBatchSize() + " config.getPersistent() " + config.getPersistent()
                + " config.getDiskStoreName() = " + config.getDiskStoreName()
                + " config.getMaximumQueueMemory() " + config.getMaximumQueueMemory()
                + " config.getDiskSynchronous() = " + config.getDiskSynchronous()
                + " config.getBatchTimeInterval() = " + config.getBatchTimeInterval()
                + " config.getBatchConflationEnabled() = " + config.getBatchConflationEnabled()
                + " config.getDispatcherThreads() = " + config.getDispatcherThreads());
          }
          factory.setBatchSize(config.getBatchSize());
          factory.setPersistent(config.getPersistent());
          factory.setDiskStoreName(config.getDiskStoreName());
          factory.setMaximumQueueMemory(config.getMaximumQueueMemory());
          factory.setDiskSynchronous(config.getDiskSynchronous());
          factory.setBatchTimeInterval(config.getBatchTimeInterval());
          factory.setBatchConflationEnabled(config.getBatchConflationEnabled());
          factory.setDispatcherThreads(config.getDispatcherThreads());
        }
        try {
          MAsyncEventListener listener =
              (MAsyncEventListener) MTableUtils.createInstance(listenerClassPath);
          asyncQueue = factory.create(queueId, listener);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        } catch (IllegalStateException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Creating asyncQueue " + e.getMessage());
          }
        }
      }
      if (asyncQueue != null) {
        result.add(asyncQueue);
      }
    }
    return result;
  }

  public static Object createInstance(String className)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Object instance;
    try {
      instance = ClassPathLoader.getLatest().forName(className).newInstance();
    } catch (InstantiationException e) {
      throw new InstantiationException("Class Instantiation failed for classname " + e);
    } catch (IllegalAccessException e) {
      throw new IllegalAccessException("Illegal Access found while Instantiating classname " + e);
    } catch (ClassNotFoundException e) {
      throw new ClassNotFoundException("classname: " + e + "not found");
    }
    return instance;
  }

  private static DiskStoreAttributes getDiskStoreAttributes(final DiskStore diskStore) {
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    diskStoreAttributes.autoCompact = diskStore.getAutoCompact();
    diskStoreAttributes.compactionThreshold = diskStore.getCompactionThreshold();
    diskStoreAttributes.allowForceCompaction = diskStore.getAllowForceCompaction();
    diskStoreAttributes.maxOplogSizeInBytes = diskStore.getMaxOplogSize();
    diskStoreAttributes.timeInterval = diskStore.getTimeInterval();
    diskStoreAttributes.writeBufferSize = diskStore.getWriteBufferSize();
    diskStoreAttributes.queueSize = diskStore.getQueueSize();
    diskStoreAttributes.diskDirs = diskStore.getDiskDirs();
    diskStoreAttributes.diskDirSizes = diskStore.getDiskDirSizes();
    diskStoreAttributes.setDiskUsageWarningPercentage(diskStore.getDiskUsageWarningPercentage());
    diskStoreAttributes.setDiskUsageCriticalPercentage(diskStore.getDiskUsageCriticalPercentage());
    diskStoreAttributes.enableDeltaPersistence = diskStore.getEnableDeltaPersistence();
    return diskStoreAttributes;
  }

  private static DiskStoreFactory getDiskStoreFactory(MonarchCacheImpl monarchCacheImpl,
      final DiskStoreAttributes diskStoreAttributes) {

    DiskStoreFactory diskStoreFactory = monarchCacheImpl.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(diskStoreAttributes.getAutoCompact());
    diskStoreFactory.setAllowForceCompaction(diskStoreAttributes.getAllowForceCompaction());
    diskStoreFactory.setCompactionThreshold(diskStoreAttributes.getCompactionThreshold());
    diskStoreFactory.setMaxOplogSize(diskStoreAttributes.getMaxOplogSizeInBytes());
    diskStoreFactory.setTimeInterval(diskStoreAttributes.getTimeInterval());
    diskStoreFactory.setWriteBufferSize(diskStoreAttributes.getWriteBufferSize());
    diskStoreFactory.setQueueSize(diskStoreAttributes.getQueueSize());
    diskStoreFactory.setDiskDirs(diskStoreAttributes.getDiskDirs());
    diskStoreFactory.setDiskDirsAndSizes(diskStoreAttributes.getDiskDirs(),
        diskStoreAttributes.getDiskDirSizes());
    diskStoreFactory
        .setDiskUsageWarningPercentage(diskStoreAttributes.getDiskUsageWarningPercentage());
    diskStoreFactory
        .setDiskUsageCriticalPercentage(diskStoreAttributes.getDiskUsageCriticalPercentage());
    diskStoreFactory.setEnableDeltaPersistence(diskStoreAttributes.getEnableDeltaPersistence());
    return diskStoreFactory;
  }

  private static void createDiskStore(MonarchCacheImpl monarchCacheImpl,
      AbstractTableDescriptor tableDescriptor) {
    String diskStoreName = tableDescriptor.getDiskStore();
    if (StringUtils.compare(diskStoreName, DEFAULT_FTABLE_DISK_STORE_NAME)
        || StringUtils.compare(diskStoreName, DEFAULT_MTABLE_DISK_STORE_NAME)) {
      return;
    }

    DiskStore diskStore = monarchCacheImpl.findDiskStore(diskStoreName);
    // disk store is already created we need to set the attributes in the table descrit
    if (diskStore != null && tableDescriptor.getDiskStoreAttributes() == null) {
      tableDescriptor.setDiskStoreAttributes(getDiskStoreAttributes(diskStore));
    } else {
      DiskStoreFactory diskStoreFactory =
          getDiskStoreFactory(monarchCacheImpl, tableDescriptor.getDiskStoreAttributes());
      diskStoreFactory.create(tableDescriptor.diskStoreName);
    }
  }

  public static Region createRegionInGeode(MonarchCacheImpl monarchCacheImpl,
      final String tableName, TableDescriptor tableDescriptor) {
    Region r = monarchCacheImpl.getRegion(tableName);
    if (r != null) {
      throw new MTableExistsException("Mtable " + tableName + " already exists");
    }
    ArrayList<AsyncEventQueue> asyncEventQueueArrayList =
        getAsyncEventArrayList(monarchCacheImpl, tableDescriptor);
    AmpoolTableRegionAttributes ampoolTableRegionAttributes = new AmpoolTableRegionAttributes();

    // Here should we decide are we creating FTable or MTable or Normal Geode region
    if (tableDescriptor != null) {
      if (tableDescriptor instanceof MTableDescriptor) {
        MTableDescriptor mTableDescriptor = (MTableDescriptor) tableDescriptor;

        // MTable related code
        if (mTableDescriptor.getUserTable()) {
          RegionFactory<MTableKey, byte[]> rf;

          if (mTableDescriptor.isDiskPersistenceEnabled()) {
            if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_DISK) {
              rf = monarchCacheImpl
                  .createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
            } else {
              rf = monarchCacheImpl.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
            }
            // By default the Disk write will be async.
            if (mTableDescriptor.getDiskWritePolicy() == MDiskWritePolicy.SYNCHRONOUS) {
              rf.setDiskSynchronous(true);
            } else {
              rf.setDiskSynchronous(false);
            }
            createDiskStore(monarchCacheImpl, mTableDescriptor);
            rf.setDiskStoreName(mTableDescriptor.getDiskStore());
          } else {
            // GEN-799
            // By default we want Regions to be OVERFLOW

            if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_DISK) {
              rf = monarchCacheImpl.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW);
            } else {
              rf = monarchCacheImpl.createRegionFactory(RegionShortcut.PARTITION);
            }
          }
          PartitionAttributesFactory paf = new PartitionAttributesFactory();

          paf.setRedundantCopies(mTableDescriptor.getRedundantCopies());

          /*
           * Set the total number of buckets to different value only if other than 113 (
           * PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT )
           */
          paf.setTotalNumBuckets(mTableDescriptor.getTotalNumOfSplits());

          if (mTableDescriptor
              .getLocalMaxMemory() != AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY) {
            paf.setLocalMaxMemory(mTableDescriptor.getLocalMaxMemory());
          } else if (mTableDescriptor
              .getLocalMaxMemoryPct() != AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT) {
            int localMaxMemoryInMB = (int) ((Runtime.getRuntime().maxMemory() / ONE_MEGA_BYTE)
                * ((float) mTableDescriptor.getLocalMaxMemoryPct() / 100));
            paf.setLocalMaxMemory(localMaxMemoryInMB);
          }

          paf.setRecoveryDelay(mTableDescriptor.getRecoveryDelay());
          paf.setStartupRecoveryDelay(mTableDescriptor.getStartupRecoveryDelay());
          if (mTableDescriptor.getTableType() == MTableType.UNORDERED) {
            ampoolTableRegionAttributes.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_UNORDERED);
          } else {
            paf.setPartitionResolver(new MTableRangePartitionResolver(
                mTableDescriptor.getTotalNumOfSplits(), mTableDescriptor.getStartRangeKey(),
                mTableDescriptor.getStopRangeKey(), mTableDescriptor.getKeySpace()));
            ampoolTableRegionAttributes
                .setRegionDataOrder(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
          }
          // paf.addPartitionListener(new RowTuplePartitionListener());
          // Attach Coprocessors to Table
          List<String> coprocessorList = mTableDescriptor.getCoprocessorList();
          MCoprocessorManager.registerCoprocessors(tableName, coprocessorList);
          // Attach Observers
          if (MCoprocessorManager.getObserversList(tableName).size() > 0) {
            rf.setCacheWriter(new PreOpRegionObserver());
            rf.addCacheListener(new PostOpRegionObserver());
          }

          rf.setPartitionAttributes(paf.create());

          if (tableDescriptor
              .getLocalMaxMemory() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY
              && tableDescriptor
                  .getLocalMaxMemoryPct() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT) {
            if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_DISK) {
              rf.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
                  EvictionAction.OVERFLOW_TO_DISK));
            } else if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER) {
              rf.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
                  EvictionAction.OVERFLOW_TO_TIER));
            } else if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.LOCAL_DESTROY) {
              rf.setEvictionAttributes(
                  EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY));
            }
          } else {
            if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_DISK) {
              rf.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(null,
                  EvictionAction.OVERFLOW_TO_DISK));
            } else if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER) {
              rf.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(null,
                  EvictionAction.OVERFLOW_TO_TIER));
            } else if (mTableDescriptor.getEvictionPolicy() == MEvictionPolicy.LOCAL_DESTROY) {
              rf.setEvictionAttributes(
                  EvictionAttributes.createLRUMemoryAttributes(null, EvictionAction.LOCAL_DESTROY));
            }
          }

          MExpirationAttributes mExpirationAttributes = mTableDescriptor.getExpirationAttributes();
          if (mExpirationAttributes != null) {
            ExpirationAction expirationAction =
                MExpirationAction.DESTROY == mExpirationAttributes.getAction()
                    ? ExpirationAction.DESTROY : ExpirationAction.INVALIDATE;
            rf.setEntryTimeToLive(
                new ExpirationAttributes(mExpirationAttributes.getTimeout(), expirationAction));
          }
          for (int i = 0; i < asyncEventQueueArrayList.size(); i++) {
            rf.addAsyncEventQueueId(asyncEventQueueArrayList.get(i).getId());
          }
          if (mTableDescriptor.getCacheLoaderClassName() != null
              && mTableDescriptor.getMaxVersions() == 1) {
            try {
              rf.setCacheLoader((CacheLoader<MTableKey, byte[]>) createInstance(
                  mTableDescriptor.getCacheLoaderClassName()));
            } catch (InstantiationException e) {
              logger.error("MTableUtils::createRegionInGeode::InstantiationException", e);
            } catch (IllegalAccessException e) {
              logger.error("MTableUtils::createRegionInGeode::IllegalAccessException", e);
            } catch (ClassNotFoundException e) {
              logger.error("MTableUtils::createRegionInGeode::ClassNotFoundException", e);
            }
          }
          rf.setCustomAttributes(ampoolTableRegionAttributes);
          rf.setRegionMapFactory("io.ampool.monarch.table.region.map.AmpoolRegionMapFactory");
          return rf.create(tableName);
        } else {
          final Region exRegion = monarchCacheImpl.getRegion(tableName);
          if (exRegion == null) {
            // create region if not present
            RegionFactory<String, MTableDescriptor> rf;
            rf = monarchCacheImpl.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
            // TODO : Check if this is required as now metaregion is created from directly
            // MTableUtils.createMetaRegion method
            if (tableName.equals(MTableUtils.AMPL_META_REGION_NAME)) {
              // if region is meta region then make it scoped to GLOBAL
              rf = rf.setScope(Scope.GLOBAL);
            }

            if (mTableDescriptor.getTableType() != null) {
              // if mtable type is set
              if (mTableDescriptor.getTableType() == MTableType.UNORDERED) {
                ampoolTableRegionAttributes.setRegionDataOrder(RegionDataOrder.ROW_TUPLE_UNORDERED);
              } else {
                ampoolTableRegionAttributes
                    .setRegionDataOrder(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED);
              }
            }
            rf.setCustomAttributes(ampoolTableRegionAttributes);
            rf.setRegionMapFactory("io.ampool.monarch.table.region.map.AmpoolRegionMapFactory");
            return rf.create(tableName);
          }
        }
      } else if (tableDescriptor instanceof FTableDescriptor) {
        // FTable related code
        // expose partitioner resolver
        // dont specify range

        FTableDescriptor fTableDescriptor = (FTableDescriptor) tableDescriptor;
        // FTable related code
        RegionFactory<MTableKey, byte[]> rf;
        // There is no need to check if disk persistence is enabled
        // as every ftable default disk store
        // For ftable always policy is to overflow
        if (fTableDescriptor.isDiskPersistenceEnabled()) {
          rf = monarchCacheImpl.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
          // By default the Disk write will be async.
          // FTable will have this option?
          // Currently making it async
          if (fTableDescriptor.getDiskWritePolicy() == MDiskWritePolicy.SYNCHRONOUS) {
            rf.setDiskSynchronous(true);
          } else {
            rf.setDiskSynchronous(false);
          }
          createDiskStore(monarchCacheImpl, fTableDescriptor);
          // this should return default disk store
          rf.setDiskStoreName(fTableDescriptor.getRecoveryDiskStore());
        } else {
          rf = monarchCacheImpl.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW);
        }

        PartitionAttributesFactory<IMKey, Object> paf = new PartitionAttributesFactory<>();
        paf.setRedundantCopies(fTableDescriptor.getRedundantCopies());

        /*
         * Set the total number of buckets to different value only if other than 113 (
         * PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT )
         */
        paf.setTotalNumBuckets(fTableDescriptor.getTotalNumOfSplits());

        // For ftable it is expected that user will give partitioner
        // or default should come as hash partioner
        PartitionResolver pr = fTableDescriptor.getPartitionResolver();
        paf.setPartitionResolver(pr == null ? MPartitionResolver.DEFAULT_RESOLVER : pr);

        if (fTableDescriptor
            .getLocalMaxMemory() != AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY) {
          paf.setLocalMaxMemory(fTableDescriptor.getLocalMaxMemory());
        } else if (fTableDescriptor
            .getLocalMaxMemoryPct() != AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT) {
          int localMaxMemoryInMB = (int) ((Runtime.getRuntime().maxMemory() / ONE_MEGA_BYTE)
              * ((float) fTableDescriptor.getLocalMaxMemoryPct() / 100));
          paf.setLocalMaxMemory(localMaxMemoryInMB);
        }

        paf.setRecoveryDelay(fTableDescriptor.getRecoveryDelay());
        paf.setStartupRecoveryDelay(fTableDescriptor.getStartupRecoveryDelay());

        // set RegionDataOrder to ftable

        ampoolTableRegionAttributes.setRegionDataOrder(RegionDataOrder.IMMUTABLE);
        // rf.setRegionDataOrder(RegionDataOrder.IMMUTABLE);

        rf.setPartitionAttributes(paf.create());

        if (fTableDescriptor.getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER) {
          // Check if tiers attached are instantiated
          Map<String, TierStoreConfiguration> stores = fTableDescriptor.getTierStores();
          if (stores != null && !stores.isEmpty()) {
            final String[] tierStores = new String[stores.size()];
            stores.keySet().toArray(tierStores);
            final Map<String, TierStore> storeMap = StoreHandler.getInstance().getTierStores();
            for (int storeIndex = 0; storeIndex < tierStores.length; storeIndex++) {
              if (!storeMap.containsKey(tierStores[storeIndex])) {
                // throw stoore not available exception
                throw new TierStoreNotAvailableException(
                    CliStrings.format("Store {0} is not available", tierStores[storeIndex]));
              }
            }
          }
          if (tableDescriptor
              .getLocalMaxMemory() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY
              && tableDescriptor
                  .getLocalMaxMemoryPct() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT) {
            rf.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_TIER));
          } else {
            rf.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(null,
                EvictionAction.OVERFLOW_TO_TIER));
          }
        } else if (fTableDescriptor.getEvictionPolicy() == MEvictionPolicy.LOCAL_DESTROY) {
          // if action is explicitly set to LOCAL_DESTROY then set accordingly
          if (tableDescriptor
              .getLocalMaxMemory() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY
              && tableDescriptor
                  .getLocalMaxMemoryPct() == AbstractTableDescriptor.DEFAULT_LOCAL_MAX_MEMORY_PCT) {
            rf.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY));
          } else {
            rf.setEvictionAttributes(
                EvictionAttributes.createLRUMemoryAttributes(null, EvictionAction.LOCAL_DESTROY));
          }
        }

        MExpirationAttributes mExpirationAttributes = fTableDescriptor.getExpirationAttributes();
        if (mExpirationAttributes != null) {
          ExpirationAction expirationAction =
              MExpirationAction.DESTROY == mExpirationAttributes.getAction()
                  ? ExpirationAction.DESTROY : ExpirationAction.INVALIDATE;
          rf.setEntryTimeToLive(
              new ExpirationAttributes(mExpirationAttributes.getTimeout(), expirationAction));
        }
        rf.setCustomAttributes(ampoolTableRegionAttributes);
        rf.setRegionMapFactory("io.ampool.monarch.table.region.map.AmpoolRegionMapFactory");
        return rf.create(tableName);
      }
    }
    return null;
  }

  /**
   * Utility API to convert given list of columns indexes to corresponding column names
   *
   * @return returns list of columns names corresponding to given column indexes
   */
  public static List<byte[]> getColumnNames(List<Integer> columns,
      TableDescriptor tableDescriptor) {
    final List<MColumnDescriptor> cds = tableDescriptor.getAllColumnDescriptors();
    return columns.stream().map(c -> cds.get(c).getColumnName()).collect(Collectors.toList());
  }

  /**
   * Get the column-index list to be retrieved during scan. In case user has provided the column
   * names convert these column names to the respective column-index using the table descriptor.
   * Once it is done the column-index list can be used for retrieving the respective columns.
   *
   * @return list of columns indexes corresponding to given column names
   */
  public static List<Integer> getColumnIds(List<byte[]> columnNames,
      TableDescriptor tableDescriptor) {
    Map<MColumnDescriptor, Integer> cdMap = tableDescriptor.getColumnDescriptorsMap();
    List<Integer> columns = columnNames.stream().sequential()
        .map(column -> cdMap.get(new MColumnDescriptor(column))).collect(Collectors.toList());
    return columns;
  }

  public static Map<Integer, Set<ServerLocation>> getBucketToServerMap(final String tableName,
      final Set<Integer> buckets, int parentOp) {
    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    Map<Integer, Set<ServerLocation>> secondaryBucketMap = new HashMap<>(113);
    MTableUtils.getLocationMap(getInternalTable(tableName), null, primaryBucketMap,
        secondaryBucketMap, parentOp);

    // Adding primary buckets
    Map<Integer, Set<ServerLocation>> allBucketMap = new HashMap<>();
    for (Map.Entry<Integer, ServerLocation> integerServerLocationEntry : primaryBucketMap
        .entrySet()) {
      Integer bucketId = integerServerLocationEntry.getKey();
      Set<ServerLocation> serverLocations = allBucketMap.get(bucketId);
      if (serverLocations == null) {
        serverLocations = new LinkedHashSet<>();
        allBucketMap.put(bucketId, serverLocations);
      }
      serverLocations.add(integerServerLocationEntry.getValue());
    }

    // Adding secondary buckets
    for (Map.Entry<Integer, Set<ServerLocation>> integerListEntry : secondaryBucketMap.entrySet()) {
      Integer bucketId = integerListEntry.getKey();
      Set<ServerLocation> serverLocations = allBucketMap.get(bucketId);
      if (serverLocations == null) {
        serverLocations = new LinkedHashSet<>();
        allBucketMap.put(bucketId, serverLocations);
      }
      for (ServerLocation serverLocation : integerListEntry.getValue()) {
        serverLocations.add(serverLocation);
      }
    }
    return allBucketMap;
  }

  public static Map<Integer, ServerLocation> getPrimaryBucketToServerMap(final String tableName,
      final Set<Integer> buckets) {
    /** for getting bucket-locations via function execution.. **/
    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    MTableImpl mTable = (MTableImpl) MClientCacheFactory.getAnyInstance().getTable(tableName);
    MTableUtils.getLocationMap(mTable, null, primaryBucketMap, null, AmpoolOpType.ANY_OP);
    return primaryBucketMap;
  }

  public static Set<Integer> getPrimaryBucketSet(final String tableName) {
    return getPrimaryBucketToServerMap(tableName, null).keySet();
  }

  /**
   * Sets the columnname or column ids whichever is not present in scan in case of selective scan.
   *
   * @return Updated scan object with both columnnames and columnids
   */
  public static Scan setColumnNameOrIds(Scan scan, TableDescriptor tableDescriptor) {
    // Handling column setting
    if (scan.getColumnNameList().size() > 0 && scan.getColumns().isEmpty()) {
      List<Integer> columnIds = getColumnIds(scan.getColumnNameList(), tableDescriptor);
      scan.setColumns(columnIds);
    } else if (scan.getColumns().size() > 0 && scan.getColumnNameList().isEmpty()) {
      scan.addColumns(getColumnNames(scan.getColumns(), tableDescriptor));
    }
    return scan;
  }

  /**
   * Helper method to execute the <code>MCountFunction</code> on each of the servers and return
   * aggregate count of entries on each server, for the region. If bucket-ids are specified then
   * only the specified buckets are used to compute the count else all primary buckets, on the
   * server, are considered. In case predicates are specified, then the count is only the number of
   * entries matching the specified predicates.
   *
   * @param table the instance of table/region
   * @param bucketIds the bucket-ids to be used for counting
   * @param predicates the predicates to be executed to filter out unwanted entries
   * @return the aggregated count matching the specified criteria
   */
  @SuppressWarnings("unchecked")
  public static long getTotalCount(final Table table, final Set<Integer> bucketIds,
      final Filter[] predicates, boolean countEvictedRecords) {
    MCountFunction.Args args =
        new MCountFunction.Args(table.getName(), bucketIds, countEvictedRecords);
    // args.setPredicates(predicates);
    args.setFilter(predicates);
    Object output = null;
    if (!MCacheFactory.getAnyInstance().isServer()) {
      output =
          FunctionService.onServers(((InternalTable) table).getInternalRegion().getRegionService())
              .withArgs(args).execute(MCountFunction.COUNT_FUNCTION).getResult();
    } else {
      output = FunctionService.onMembers().withArgs(args).execute(MCountFunction.COUNT_FUNCTION)
          .getResult();
    }
    assert output instanceof List;
    return ((List<Object>) output).stream().mapToLong(e -> (Long) e).sum();
  }

  /**
   * for getting bucket-locations via function execution..
   **/
  @SuppressWarnings("unchecked")
  public static void getLocationAndCount(final Table table,
      Map<Integer, Pair<ServerLocation, Long>> primaryMap,
      Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap) {
    MGetMetadataFunction.Args args = new MGetMetadataFunction.Args(table.getName());
    Object output = null;
    if (!MCacheFactory.getAnyInstance().isServer()) {
      output =
          FunctionService.onServers(((InternalTable) table).getInternalRegion().getRegionService())
              .withArgs(args).execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
    } else {
      output = FunctionService.onMembers().withArgs(args)
          .execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
    }
    for (Object[] objects : ((List<Object[]>) output)) {
      primaryMap.putAll((Map) objects[0]);
      if (secondaryMap != null) {
        Map<Integer, Set<Pair<ServerLocation, Long>>> map = (Map) objects[1];
        for (Map.Entry<Integer, Set<Pair<ServerLocation, Long>>> entry : map.entrySet()) {
          if (secondaryMap.containsKey(entry.getKey())) {
            secondaryMap.get(entry.getKey()).addAll(entry.getValue());
          } else {
            secondaryMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<ServerLocation, List<Integer>> getLocationAndSize(final Table table,
      Map<Integer, Pair<ServerLocation, Long>> primaryMap,
      Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap) {
    MGetMetadataFunction.Args args =
        new MGetMetadataFunction.Args(table.getName(), null, MGetMetadataFunction.Opt.SIZE);
    // args.setParentOp(AmpoolOpType.SCAN_OP);
    Object output;
    if (!MCacheFactory.getAnyInstance().isServer()) {
      output =
          FunctionService.onServers(((InternalTable) table).getInternalRegion().getRegionService())
              .withArgs(args).execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
    } else {
      output = FunctionService.onMembers().withArgs(args)
          .execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();
    }
    Map<ServerLocation, List<Integer>> serverToBucketMap = new HashMap<>(10);
    for (Object[] objects : ((List<Object[]>) output)) {
      primaryMap.putAll((Map) objects[0]);
      if (objects.length > 2) {
        serverToBucketMap.putAll((Map) objects[2]);
      }
      if (secondaryMap != null) {
        Map<Integer, Set<Pair<ServerLocation, Long>>> map = (Map) objects[1];
        for (Map.Entry<Integer, Set<Pair<ServerLocation, Long>>> entry : map.entrySet()) {
          if (secondaryMap.containsKey(entry.getKey())) {
            secondaryMap.get(entry.getKey()).addAll(entry.getValue());
          } else {
            secondaryMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    return serverToBucketMap;
  }

  @SuppressWarnings("unchecked")
  public static void getLocationMap(final Table table, Set<Integer> buckets,
      Map<Integer, ServerLocation> primaryMap, Map<Integer, Set<ServerLocation>> secondaryMap,
      int parentOp) {
    MGetMetadataFunction.Args args = new MGetMetadataFunction.Args(table.getName(), buckets);
    args.setParentOp(parentOp);

    Object output =
        FunctionService.onServers(((InternalTable) table).getInternalRegion().getRegionService())
            .withArgs(args).execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();

    /** extract only server-location from the original map **/
    for (Object[] objects : ((List<Object[]>) output)) {
      if (primaryMap != null) {
        ((Map<Integer, Pair<ServerLocation, Long>>) objects[0])
            .forEach((k, v) -> primaryMap.put(k, v.getFirst()));
      }
      if (secondaryMap != null) {
        for (Map.Entry<Integer, Set<Pair<ServerLocation, Long>>> integerSetEntry : ((Map<Integer, Set<Pair<ServerLocation, Long>>>) objects[1])
            .entrySet()) {
          Set<Pair<ServerLocation, Long>> value = integerSetEntry.getValue();
          Set<ServerLocation> locationSet = new LinkedHashSet<>();
          for (Pair<ServerLocation, Long> serverLocationLongPair : value) {
            locationSet.add(serverLocationLongPair.getFirst());
          }
          secondaryMap.put(integerSetEntry.getKey(), locationSet);
        }
      }
    }
  }

  /**
   * Helper routine to convert an integer key into a row key. Useful when your keys and partition
   * ranges are integer based.
   *
   * @param intKey the integer key.
   * @return the integer key in byte format.
   */
  public static byte[] integerToKey(int intKey) {
    return Bytes.intToBytesFlipped(intKey);
  }

  /**
   * Helper routine to convert a long integer key into a row key. Useful when your keys and
   * partition ranges are long integer based.
   *
   * @param longKey the long integer key.
   * @return the long integer key in byte format.
   */
  public static byte[] longIntegerToKey(long longKey) {
    return Bytes.longToBytesFlipped(longKey);
  }

  /**
   * Returns true if the table is empty.
   *
   * @param table the mtable.
   * @return true if table is empty.
   */

  public static boolean isTableEmpty(MTableImpl table) {
    try {
      Region<?, ?> region = table.getTableRegion();
      Function function = new TableIsEmptyFunction();

      FunctionService.registerFunction(function);

      ResultCollector<?, ?> rc = FunctionService.onRegion(region)
          .withCollector(new TableIsEmptyResultCollector()).execute(function);

      return (Boolean) rc.getResult();
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      throw ge;
    }
  }

  /**
   * Return the deep hash-code of the object. If the object is an the hash-code is derived from each
   * element in the array, recursively, so that it is always consistent. For primitive objects
   * (int/long) or map it is direct hash code.
   *
   * @param object the object of which hash-code is to be returned
   * @return the hash-code of the provided object
   */
  public static int getDeepHashCode(final Object object) {
    int result;
    if (object == null) {
      result = 0;
    } else if (object instanceof byte[]) {
      result = Arrays.hashCode((byte[]) object);
    } else if (object instanceof short[]) {
      result = Arrays.hashCode((short[]) object);
    } else if (object instanceof int[]) {
      result = Arrays.hashCode((int[]) object);
    } else if (object instanceof long[]) {
      result = Arrays.hashCode((long[]) object);
    } else if (object instanceof char[]) {
      result = Arrays.hashCode((char[]) object);
    } else if (object instanceof float[]) {
      result = Arrays.hashCode((float[]) object);
    } else if (object instanceof double[]) {
      result = Arrays.hashCode((double[]) object);
    } else if (object instanceof boolean[]) {
      result = Arrays.hashCode((boolean[]) object);
    } else if (object instanceof Object[] || object.getClass().isArray()) {
      int len = Array.getLength(object);
      Object element;
      result = 1;
      for (int i = 0; i < len; i++) {
        element = Array.get(object, i);
        result = 31 * result + (element == null ? 0
            : (element instanceof Object[] || element.getClass().isArray()
                ? getDeepHashCode(element) : element.hashCode()));
      }
    } else if (object instanceof Map) {
      result = 0;
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) object).entrySet()) {
        result += (MTableUtils.getDeepHashCode(entry.getKey())
            ^ MTableUtils.getDeepHashCode(entry.getValue()));
      }
    } else {
      result = object.hashCode();
    }
    return result;
  }


  public static StorageFormatter getStorageFormatter(TableDescriptor tableDescriptor) {
    byte[] magicNoEncodingIdReservedBytes =
        ((AbstractTableDescriptor) tableDescriptor).getStorageFormatterIdentifiers();
    return StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
        magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[2]);
  }

  public static StorageFormatter getStorageFormatter(byte[] magicNoEncodingIdReservedBytes) {
    if (magicNoEncodingIdReservedBytes.length < 4) {
      throw new MException(
          "Invalid storage identifiers. StorageIdentifiers are need to be of size 4 bytes");
    }
    return StorageFormatters.getInstance(magicNoEncodingIdReservedBytes[0],
        magicNoEncodingIdReservedBytes[1], magicNoEncodingIdReservedBytes[3]);
  }

  private Row getRow(final byte[] bytes, final MTableDescriptor td, final List<Integer> columns)
      throws IOException, ClassNotFoundException, InterruptedException {
    return null;
  }


  /**
   * Changes the old version value of specific column using the value specified for a column in the
   * new version. Data types for both the types must be same.
   *
   * @param oldValue old value in the region map that will be changed to add new version
   * @param mTableDescriptor table descriptor
   * @param put put object with new values
   * @param columnName1 Name of the column used for getting new value
   * @param columnName2 Name of the column whose old value is getting changed
   * @param getNewValue function provided to get the new value
   */
  public static void changeOldVersionValue(final Object oldValue,
      final MTableDescriptor mTableDescriptor, final Put put, final String columnName1,
      final String columnName2, BiFunction<BasicTypes, Object, Object> getNewValue) {

    // 1. check old value for multiversion instance
    if (oldValue != null && oldValue instanceof MultiVersionValue) {
      MultiVersionValue oldMultiVersionValue = (MultiVersionValue) oldValue;
      byte[][] versions = oldMultiVersionValue.getVersions();
      // 2. Check for number of versions
      if (versions.length > 0) {
        // get the older version which needs to be updated and column descriptors.
        // -2 is needed as we are updating the oldvalue with the new version
        byte[] oldVersion = versions[versions.length - 1];
        MColumnDescriptor mColumnDescriptor1 = mTableDescriptor.getColumnDescriptors().stream()
            .filter(CD -> CD.getColumnNameAsString().equals(columnName1)).findFirst().get();
        MColumnDescriptor mColumnDescriptor2 = mTableDescriptor.getColumnDescriptors().stream()
            .filter(CD -> CD.getColumnNameAsString().equals(columnName2)).findFirst().get();

        if (oldVersion != null && mColumnDescriptor1 != null && mColumnDescriptor2 != null) {
          // 4. get column1 value and column2 value
          Object column1Value =
              put.getColumnValueMap().get(new ByteArrayKey(mColumnDescriptor1.getColumnName()));
          Object column2Value =
              getNewValue.apply((BasicTypes) mColumnDescriptor1.getColumnType(), column1Value);
          if (column1Value != null && column2Value != null) {
            // fixed length column
            byte[] column2ValBytes = mColumnDescriptor1.getColumnType().serialize(column2Value);
            // get the bitmap
            IBitMap oldValueBitmap =
                MTableStorageFormatter.readBitMap(mTableDescriptor, oldVersion);
            if (mColumnDescriptor1.getColumnType().isFixedLength()) {
              // if the column is already present the older version we just need to update the value
              // in place
              if (oldValueBitmap.get(mColumnDescriptor2.getIndex())) {
                // bit is already set in the old value we just need to update the value
                // get the position for the column
                Iterator<Integer> iterator = mTableDescriptor.getFixedLengthColumns().iterator();

                int offset = BITMAP_START_POS + mTableDescriptor.getBitMapLength();
                // get the column value offset by iterating the fixed length columns
                while (iterator.hasNext()) {
                  int colIndex = iterator.next();
                  if (colIndex == mColumnDescriptor2.getIndex()) {
                    break;
                  }
                  if (oldValueBitmap.get(colIndex)) {
                    int lengthOfByteArray = mTableDescriptor.getColumnDescriptorByIndex(colIndex)
                        .getColumnType().lengthOfByteArray();
                    offset += lengthOfByteArray;
                  }
                }
                System.arraycopy(column2ValBytes, 0, oldVersion, offset, column2ValBytes.length);
                logger.info(
                    "Updated the old version value in place with the new value for fixed length column");
              } else { // column is not there in the older version
                // need to create new byte[] with adding new column and value
                byte[] newVerison = new byte[oldVersion.length + column2ValBytes.length];
                // write the row header
                System.arraycopy(oldVersion, 0, newVerison, 0,
                    mTableDescriptor.getRowHeaderBytes().length);
                int offset = mTableDescriptor.getRowHeaderBytes().length;
                // write the timestamp
                System.arraycopy(oldVersion, offset, newVerison, offset, Bytes.SIZEOF_LONG);
                offset += Bytes.SIZEOF_LONG;
                oldValueBitmap.set(mColumnDescriptor2.getIndex());
                System.arraycopy(oldValueBitmap.toByteArray(), 0, newVerison, offset,
                    oldValueBitmap.toByteArray().length);
                offset += oldValueBitmap.toByteArray().length;
                Iterator<Integer> iterator = mTableDescriptor.getFixedLengthColumns().iterator();
                // get the column value offset by iterating the fixed length columns
                int oldverisonOffset = offset;
                while (iterator.hasNext()) {
                  int colIndex = iterator.next();
                  if (oldValueBitmap.get(colIndex)) {
                    if (colIndex == mColumnDescriptor2.getIndex()) {
                      System.arraycopy(column2ValBytes, 0, newVerison, offset,
                          column2ValBytes.length);
                      offset += column2ValBytes.length;
                    } else {
                      int lengthOfByteArray = mTableDescriptor.getColumnDescriptorByIndex(colIndex)
                          .getColumnType().lengthOfByteArray();
                      // copy value to new byte array
                      System.arraycopy(oldVersion, oldverisonOffset, newVerison, offset,
                          lengthOfByteArray);
                      offset += lengthOfByteArray;
                      oldverisonOffset += lengthOfByteArray;
                    }
                  } // end if
                } // end while
                // now append all the remaining data for the variable length columns
                int numColumns = (int) mTableDescriptor.getVaribleLengthColumns().stream()
                    .filter(integer -> oldValueBitmap.get(integer)).count();

                if (numColumns > 0) {
                  // get the varaible length offset
                  int varstartOffset = oldVersion.length - numColumns * Bytes.SIZEOF_INT;
                  // copy all the variable length column data
                  System.arraycopy(oldVersion, oldverisonOffset, newVerison, offset,
                      varstartOffset - oldverisonOffset);
                  offset = newVerison.length - numColumns * Bytes.SIZEOF_INT;
                  for (int i = 0; i < numColumns; i++) {
                    int columnOffsetValue =
                        Bytes.toInt(oldVersion, varstartOffset) + column2ValBytes.length;
                    Bytes.putInt(newVerison, offset, columnOffsetValue);
                    varstartOffset += Bytes.SIZEOF_INT;
                    offset += Bytes.SIZEOF_INT;
                  }
                }
                // set the new version in the mutiversion value
                oldMultiVersionValue.getVersions()[oldMultiVersionValue.getVersions().length - 1] =
                    newVerison;
                logger.info(
                    "Updated the old version value new byte array version for fixed length column");
              }

            } else { // column is variable length
              if (oldValueBitmap.get(mColumnDescriptor2.getIndex())) {

                // get the offsets and length map
                List<Integer> varLengthColumns = mTableDescriptor.getVaribleLengthColumns();
                int offsetPosition = oldVersion.length - Bytes.SIZEOF_INT;
                int numOfVarColsInCurrentValue = (int) varLengthColumns.stream()
                    .filter(integer -> oldValueBitmap.get(integer)).count();

                int totalOffSetOverhead = numOfVarColsInCurrentValue * Bytes.SIZEOF_INT;
                int offsetStartingPos = (oldVersion.length - totalOffSetOverhead);
                Map<Integer, Pair<Integer, Integer>> posToOffsetMap = new HashMap<>();
                for (int i = 0; i < varLengthColumns.size(); i++) {
                  final Integer colIndex = varLengthColumns.get(i);
                  if (oldValueBitmap.get(colIndex)) {
                    Pair<Integer, Integer> offsets = null;
                    if ((oldVersion.length - totalOffSetOverhead) == (offsetPosition)) {
                      // this is last set bit
                      offsets =
                          new Pair(Bytes.toInt(oldVersion, offsetPosition), offsetStartingPos);
                    } else {
                      offsets = new Pair(Bytes.toInt(oldVersion, offsetPosition),
                          Bytes.toInt(oldVersion, offsetPosition - Bytes.SIZEOF_INT));

                    }
                    posToOffsetMap.put(colIndex, offsets);
                    offsetPosition -= Bytes.SIZEOF_INT;
                  }
                }

                // need to create new byte[] with adding new column and value
                byte[] newVerison = new byte[oldVersion.length + column2ValBytes.length
                    - (posToOffsetMap.get(mColumnDescriptor2.getIndex()).getSecond()
                        - posToOffsetMap.get(mColumnDescriptor2.getIndex()).getFirst())];

                // write the row header
                System.arraycopy(oldVersion, 0, newVerison, 0,
                    mTableDescriptor.getRowHeaderBytes().length);
                int offset = mTableDescriptor.getRowHeaderBytes().length;
                // write the timestamp
                System.arraycopy(oldVersion, offset, newVerison, offset, Bytes.SIZEOF_LONG);
                offset += Bytes.SIZEOF_LONG;
                oldValueBitmap.set(mColumnDescriptor2.getIndex());
                System.arraycopy(oldValueBitmap.toByteArray(), 0, newVerison, offset,
                    oldValueBitmap.toByteArray().length);
                offset += oldValueBitmap.toByteArray().length;
                Iterator<Integer> iterator = mTableDescriptor.getFixedLengthColumns().iterator();
                // get the column value offset by iterating the fixed length columns
                while (iterator.hasNext()) {
                  int colIndex = iterator.next();
                  if (oldValueBitmap.get(colIndex)) {
                    int lengthOfByteArray = mTableDescriptor.getColumnDescriptorByIndex(colIndex)
                        .getColumnType().lengthOfByteArray();
                    // copy value to new byte array
                    System.arraycopy(oldVersion, offset, newVerison, offset, lengthOfByteArray);
                    offset += lengthOfByteArray;
                  } // end if
                } // end while
                // now append all the remaining data for the variable length columns

                int newColumnOffset = newVerison.length - Bytes.SIZEOF_INT;
                Iterator<Integer> varlenColItr = varLengthColumns.iterator();
                while (varlenColItr.hasNext()) {
                  int colIndex = varlenColItr.next();
                  if (oldValueBitmap.get(colIndex)) {
                    if (colIndex == mColumnDescriptor2.getIndex()) {
                      // write value
                      System.arraycopy(column2ValBytes, 0, newVerison, offset,
                          column2ValBytes.length);
                      // write offset
                      System.arraycopy(BasicTypes.INT.serialize(offset), 0, newVerison,
                          newColumnOffset, Bytes.SIZEOF_INT);
                      offset += column2ValBytes.length;
                      newColumnOffset -= Bytes.SIZEOF_INT;
                    } else {
                      Pair<Integer, Integer> offsetLen = posToOffsetMap.get(colIndex);
                      int len = offsetLen.getSecond() - offsetLen.getFirst();
                      // write value
                      System.arraycopy(oldVersion, offsetLen.getFirst(), newVerison, offset, len);
                      // write offset
                      System.arraycopy(BasicTypes.INT.serialize(offset), 0, newVerison,
                          newColumnOffset, Bytes.SIZEOF_INT);
                      offset += len;
                      newColumnOffset -= Bytes.SIZEOF_INT;
                    }
                  }
                } // end while
                // set the new version in the mutiversion value
                oldMultiVersionValue.getVersions()[oldMultiVersionValue.getVersions().length - 1] =
                    newVerison;
                logger.info(
                    "Updated the old version with new byte array for variable length column and column is present in the old version");
              } else { // column is not there in the older version
                // need to create new byte[] with adding new column and value
                byte[] newVerison =
                    new byte[oldVersion.length + column2ValBytes.length + Bytes.SIZEOF_INT];

                // get the offsets and length map
                List<Integer> varLengthColumns = mTableDescriptor.getVaribleLengthColumns();
                int offsetPosition = oldVersion.length - Bytes.SIZEOF_INT;
                int numOfVarColsInCurrentValue = (int) varLengthColumns.stream()
                    .filter(integer -> oldValueBitmap.get(integer)).count();

                int totalOffSetOverhead = numOfVarColsInCurrentValue * Bytes.SIZEOF_INT;
                int offsetStartingPos = (oldVersion.length - totalOffSetOverhead);
                Map<Integer, Pair<Integer, Integer>> posToOffsetMap = new HashMap<>();
                for (int i = 0; i < varLengthColumns.size(); i++) {
                  final Integer colIndex = varLengthColumns.get(i);
                  if (oldValueBitmap.get(colIndex)) {
                    Pair<Integer, Integer> offsets = null;
                    if ((oldVersion.length - totalOffSetOverhead) == (offsetPosition)) {
                      // this is last set bit
                      offsets =
                          new Pair(Bytes.toInt(oldVersion, offsetPosition), offsetStartingPos);
                    } else {
                      offsets = new Pair(Bytes.toInt(oldVersion, offsetPosition),
                          Bytes.toInt(oldVersion, offsetPosition - Bytes.SIZEOF_INT));

                    }
                    posToOffsetMap.put(colIndex, offsets);
                    offsetPosition -= Bytes.SIZEOF_INT;
                  }
                }

                // write the row header
                System.arraycopy(oldVersion, 0, newVerison, 0,
                    mTableDescriptor.getRowHeaderBytes().length);
                int offset = mTableDescriptor.getRowHeaderBytes().length;
                // write the timestamp
                System.arraycopy(oldVersion, offset, newVerison, offset, Bytes.SIZEOF_LONG);
                offset += Bytes.SIZEOF_LONG;
                oldValueBitmap.set(mColumnDescriptor2.getIndex());
                System.arraycopy(oldValueBitmap.toByteArray(), 0, newVerison, offset,
                    oldValueBitmap.toByteArray().length);
                offset += oldValueBitmap.toByteArray().length;
                Iterator<Integer> iterator = mTableDescriptor.getFixedLengthColumns().iterator();
                // get the column value offset by iterating the fixed length columns
                while (iterator.hasNext()) {
                  int colIndex = iterator.next();
                  if (oldValueBitmap.get(colIndex)) {
                    int lengthOfByteArray = mTableDescriptor.getColumnDescriptorByIndex(colIndex)
                        .getColumnType().lengthOfByteArray();
                    // copy value to new byte array
                    System.arraycopy(oldVersion, offset, newVerison, offset, lengthOfByteArray);
                    offset += lengthOfByteArray;
                  } // end if
                } // end while
                // now append all the remaining data for the variable length columns

                int newColumnOffset = newVerison.length - Bytes.SIZEOF_INT;
                Iterator<Integer> varlenColItr = varLengthColumns.iterator();
                while (varlenColItr.hasNext()) {
                  int colIndex = varlenColItr.next();
                  if (oldValueBitmap.get(colIndex)) {
                    if (colIndex == mColumnDescriptor2.getIndex()) {
                      // write value
                      System.arraycopy(column2ValBytes, 0, newVerison, offset,
                          column2ValBytes.length);
                      // write offset
                      System.arraycopy(BasicTypes.INT.serialize(offset), 0, newVerison,
                          newColumnOffset, Bytes.SIZEOF_INT);
                      offset += column2ValBytes.length;
                      newColumnOffset -= Bytes.SIZEOF_INT;
                    } else {
                      Pair<Integer, Integer> offsetLen = posToOffsetMap.get(colIndex);
                      int len = offsetLen.getSecond() - offsetLen.getFirst();
                      // write value
                      System.arraycopy(oldVersion, offsetLen.getFirst(), newVerison, offset, len);
                      // write offset
                      System.arraycopy(BasicTypes.INT.serialize(offset), 0, newVerison,
                          newColumnOffset, Bytes.SIZEOF_INT);
                      offset += len;
                      newColumnOffset -= Bytes.SIZEOF_INT;
                    }
                  }
                } // end while
                // set the new version in the mutiversion value
                oldMultiVersionValue.getVersions()[oldMultiVersionValue.getVersions().length - 1] =
                    newVerison;
                logger
                    .info("Updated the old version with new byte array for variable length column");
              }
            }
          } else {
            logger.error("Column values are not available");
            return;
          }

        } else {
          logger.error("Wither Old version is not there or columns are not availble");
          return;
        }
        // MTableStorageFormatter.readBitMap()
      } else {// only one version no need for update
        logger.info("Only one version is availble.");
      }
    } else { // else no update as no value is there
      logger.info("Value is null.");
    }


  }

  public static void setAmpoolOpInfoInEvent(EntryEventImpl event) {

    if (AmpoolTableRegionAttributes.isAmpoolMTable(event.getRegion().getCustomAttributes())) {
      MOpInfo opInfo = null;
      Object newValue = event.getValue();
      boolean isMVVValue = false;
      if (newValue instanceof VMCachedDeserializable) {
        // /** extract the byte-array **/
        Object tempVal;
        tempVal = ((VMCachedDeserializable) newValue).getDeserializedForReading();

        if (tempVal instanceof MValue) {
          opInfo = MOpInfo.fromValue((MValue) tempVal);
        } else if (tempVal instanceof List) {
          opInfo = MOpInfo.fromValue((MValue) ((List) tempVal).get(0));
          isMVVValue = true;
        }
      } else if (event.getKey() instanceof MTableKey && newValue instanceof byte[]) {
        // case of delete the column list is in the key
        Object key = event.getKey();
        opInfo = MOpInfo.fromKey((MTableKey) key);
      } else if (newValue instanceof MValue) {
        opInfo = MOpInfo.fromValue((MValue) newValue);
      } else if (newValue instanceof List) {
        // single key muliple values
        opInfo = MOpInfo.fromValue((MValue) ((List) newValue).get(0));
        isMVVValue = true;
      }

      /*
       * else {
       *
       * throw new RuntimeException("Fix me new format " + newValue.getClass().getName()); }
       */
      if (opInfo != null && opInfo.getTimestamp() == 0 && !event.isOriginRemote()
          && !(isMVVValue)) {
        // set timestamp for future put use
        long timestamp = System.nanoTime();
        event.setCallbackArgument(timestamp);
      }
      event.setOpInfo(opInfo);
    }
  }

}
