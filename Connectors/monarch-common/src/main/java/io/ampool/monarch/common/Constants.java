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
package io.ampool.monarch.common;

public class Constants {
  /**
   * Security configuration.
   */
  public static final class Security {
    public static final String AUTH_SERVER_ADDRESS = "";
    public static final String AUTH_SERVER_BIND_ADDRESS = "";
  }

  public static final class MonarchThriftService {
    public static final String THRIFT_SERVICE_PORT = "ampool.monarch.thrift.service.port";
    public static final String THRIFT_BIND_ADDRESS = "ampool.monarch.thrift.bind.address";
  }

  public static final class MonarchServer {
    public static final String MONARCH_SERVER_PORT = "ampool.monarch.server.port";
    public static final String MONARCH_SERVER_ADDRESS = "ampool.monarch.server.address";
  }

  public static final class MonarchLocator {
    public static final String MONARCH_LOCATOR_PORT = "ampool.monarch.locator.port";
    public static final String MONARCH_LOCATOR_ADDRESS = "ampool.monarch.locator.address";
    public static final String MONARCH_CLIENT_CACHE_TYPE = "ampool.monarch.client.cache.type";
  }

  public static final class MonarchPersistanceConfig {
    public static final String MONARCH_HBASE_PERSISTANCE = "ampool.monarch.hbase.persistance";
  }

  public static final class MonarchHBaseStoreConfig {
    public static final String MONARCH_PERSIST_HBASE_ROOT_DIR = "hbase.rootdir";
    public static final String MONARCH_PERSIST_HBASE_CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";
    public static final String MONARCH_PERSIST_HBASE_ZOOKEEOER_QUORUM = "hbase.zookeeper.quorum";
    public static final String MONARCH_PERSIST_HBASE_DFS_REPLICATION = "dfs.replication";
    public static final String MONARCH_PERSIST_HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    //public static final String MONARCH_PERSIST_HBASE_ZOOKEEPER_DATADIR = "hbase.zookeeper.property.dataDir";

  }

  public static final class MClientCacheconfig {
    public static final String MONARCH_CLIENT_LOG = "ampool.monarch.client.log";
  }

}
