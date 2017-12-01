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

package io.ampool

/**
  * Created on: 2016-03-02
  * Since version: 0.3.2.0
  */
object Constants extends Serializable {
  /** constants used.. **/
  final val AmpoolLocatorHostKey = "ampool.locator.host"
  final val AmpoolLocatorPortKey = "ampool.locator.port"
  final val AmpoolBatchSize = "ampool.batch.size"
  final val AmpoolLogFile = "ampool.log.file"
  final val AmpoolReadTimeout = "ampool.read.timeout"
  final val AmpoolReadTimeoutDefault = "90000"
  /* defaults that can be specified at SparkContext level */
  val ContextDefaults: Array[String] = Array(
    Constants.AmpoolLocatorHostKey, Constants.AmpoolLocatorPortKey,
    Constants.AmpoolBatchSize, Constants.AmpoolLogFile, Constants.AmpoolReadTimeout)

  final val AmpoolTableType = "ampool.table.type"
  final val AmpoolEnablePersistence = "ampool.enable.persistence"
  final val AmpoolRedundancy = "ampool.table.redundancy"
  final val AmpoolBatchPrefix = "ampool.batch.prefix"
  final val AmpoolKeyColumns = "ampool.key.columns"
  final val AmpoolPartitioningColumn = "ampool.partitioning.column"
  final val AmpoolKeySeparator = "ampool.key.separator"

  final val AmpoolMaxVersions = "ampool.max.versions"
  final val AmpoolDefaultMaxVersions = "1"
  final val AmpoolReadFilterOnLatestVersion = "ampool.read.filter.latest"
  final val AmpoolReadOldestFirst = "ampool.read.oldest.first"

  /** related to meta-table maintaining counter **/
  final val AmpoolTableAppend = "ampool.table.append"
  final val MetaTableSuffix = "_meta__"
  final val MetaKeyPrefix = "count-"
  final val MetaColumn = "Count"

  /** related to security - kerberos authC **/
  final val KerberosServicePrincipal = "security.kerberos.service.principal";
  final val KerberosSecurityUsername = "security.kerberos.user.principal";
  final val KerberosSecurityPassword = "security.kerberos.user.keytab";
  final val KerberosAuthCEnabled= "security.enable.kerberos.authc";
}
