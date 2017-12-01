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
package io.ampool.tierstore.config;

public interface CommonConfig {
  String TABLE_NAME = "table.name";
  String PARTITION_ID = "partition.id";
  String BASE_URI = "base.uri";
  String HADOOP_SITE_XML_PATH = "hadoop.site.xml.path";
  String HDFS_SITE_XML_PATH = "hdfs.site.xml.path";
  String TIME_BASED_PARTITIONING = "time.based.partitioning.enabled";
  String PARTITIIONING_INTERVAL_MS = "partitioning.interval.ms";
  String TABLE_PATH_PARTS = "table.path.parts";
  String TABLE_PATH_PARTS_SEPARATOR = "/";

  // security properties
  String USER_NAME = "security.user.name";
  String KEYTAB_PATH = "security.keytab.path";
}
