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

package io.ampool.monarch.table.security;

import org.apache.geode.security.ResourcePermission;
import org.apache.shiro.authz.Permission;

import java.util.ArrayList;
import java.util.List;

public class MashTestCommand {

  public static ResourcePermission none = null;
  public static ResourcePermission everyOneAllowed = new ResourcePermission();
  public static ResourcePermission dataRead = new ResourcePermission("DATA", "READ");
  public static ResourcePermission dataWrite = new ResourcePermission("DATA", "WRITE");
  public static ResourcePermission dataManage = new ResourcePermission("DATA", "MANAGE");

  public static ResourcePermission regionARead = new ResourcePermission("DATA", "READ", "RegionA");
  public static ResourcePermission regionAWrite =
      new ResourcePermission("DATA", "WRITE", "RegionA");
  public static ResourcePermission regionAManage =
      new ResourcePermission("DATA", "MANAGE", "RegionA");

  public static ResourcePermission clusterRead = new ResourcePermission("CLUSTER", "READ");
  public static ResourcePermission clusterWrite = new ResourcePermission("CLUSTER", "WRITE");
  public static ResourcePermission clusterManage = new ResourcePermission("CLUSTER", "MANAGE");

  private static List<MashTestCommand> mashCommands = new ArrayList<>();

  static {
    init();
  }

  private final String command;
  private final ResourcePermission permission;

  public MashTestCommand(String command, ResourcePermission permission) {
    this.command = command;
    this.permission = permission;
  }

  private static void createTestCommand(String command, ResourcePermission permission) {
    MashTestCommand instance = new MashTestCommand(command, permission);
    mashCommands.add(instance);
  }

  public String getCommand() {
    return this.command;
  }

  public ResourcePermission getPermission() {
    return this.permission;
  }

  public static List<MashTestCommand> getCommands() {
    return mashCommands;
  }

  public static List<MashTestCommand> getPermittedCommands(Permission permission) {
    List<MashTestCommand> result = new ArrayList<>();
    for (MashTestCommand mashCommand : mashCommands) {
      ResourcePermission cPerm = mashCommand.getPermission();
      if (cPerm != null && permission.implies(cPerm)) {
        result.add(mashCommand);
      }
    }
    return result;
  }

  private static void init() {

    createTestCommand(
        "create table --name=abc --type=UNORDERED --columns=c1,c2,c3 --disk-persistence=false",
        dataManage);
    createTestCommand("delete table --name=abc", dataManage);
    createTestCommand("describe table --name=tableName", clusterRead);
    createTestCommand("list tables", dataRead);

    // createTestCommand("create tier-store --name=sampleTierStore
    // --handler=io.ampool.tierstore.stores.LocalTierStore
    // --reader=io.ampool.tierstore.readers.orc.TierStoreORCReader
    // --writer=io.ampool.tierstore.writers.orc.TierStoreORCWriter",dataManage);
    createTestCommand("list tier-stores", clusterRead);
    createTestCommand("describe tier-store --name=DefaultLocalORCStore", clusterRead);
    // createTestCommand("destroy tier-store --name=DefaultLocalORCStore", clusterRead);
    createTestCommand(
        "tput --key=1 --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/RegionA",
        regionAWrite);
    createTestCommand("tappend --table=/RegionA --value-json={'ID':['3']}", regionAWrite);
    createTestCommand("tdelete --table=RegionA --key=1 ", regionAWrite);
    createTestCommand("tget --table=RegionA --key=1", regionARead);
    createTestCommand("tscan --table=RegionA", regionARead);
  }
}
