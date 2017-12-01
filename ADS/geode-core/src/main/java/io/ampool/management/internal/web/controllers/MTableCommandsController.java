/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.ampool.management.internal.web.controllers;

import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.CreateMTableFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.web.controllers.AbstractCommandsController;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The PdxCommandsController class implements GemFire Management REST API web service endpoints for
 * Gfsh PDX Commands.
 * 
 * @see org.apache.geode.management.internal.cli.commands.PDXCommands
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see Controller
 * @see RequestMapping
 * @see RequestMethod
 * @see RequestParam
 * @see ResponseBody
 * @since 8.0
 */
@Controller("mTableController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class MTableCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.POST, value = "/mtables")
  @ResponseBody
  public String createMtable(@RequestParam("name") final String mTableName,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__TYPE,
          required = true) final MTableType mTableType,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE_COLUMNS,
          required = true) final String[] mTableColumns,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__REDUNDANT_COPIES, defaultValue = "1",
          required = false) final Integer nRedundantCopies,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__DISK_PERSISTENCE, defaultValue = "false",
          required = false) final Boolean enableDiskPersistence,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__MAX_VERSIONS, defaultValue = "1",
          required = false) Integer nMaxVersions,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__DISK_WRITE_POLICY,
          required = false) MDiskWritePolicy mDiskWritePolicy,
      @RequestParam(value = MashCliStrings.CREATE_MTABLE__DISKSTORE,
          required = false) final String diskStoreName) {

    Logger logger = LogService.getLogger();

    try {

      try {
        MTableUtils.isTableNameLegal(mTableName);
      } catch (IllegalArgumentException e) {
        return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
            (CommandResult) ResultBuilder.createGemFireErrorResult("Invalid Table Name "
                + CliStrings.format(MashCliStrings.CREATE_MTABLE__NAME__HELP))));
      }

      MTableDescriptor tableDescriptor = new MTableDescriptor(mTableType);
      List<String> columnNames = Arrays.asList(mTableColumns);
      for (int i = 0; i < columnNames.size(); i++) {
        tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(i)));
      }
      Set<DistributedMember> targetMembers = null;
      String[] groups1 = null;
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {

        return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
            (CommandResult) ResultBuilder.createGemFireErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0))));
      }
      if (nRedundantCopies != null && ((nRedundantCopies >= 0) && (nRedundantCopies <= 3))) {

        tableDescriptor.setRedundantCopies(nRedundantCopies);
      } else {

        return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
            (CommandResult) ResultBuilder.createGemFireErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0))));
      }
      tableDescriptor.setMaxVersions(nMaxVersions);

      if (enableDiskPersistence) {
        if (mDiskWritePolicy == null) {
          mDiskWritePolicy = MDiskWritePolicy.ASYNCHRONOUS;
        }
        tableDescriptor.enableDiskPersistence(mDiskWritePolicy);
        if (diskStoreName != null) {
          if (!diskStoreExists(MCacheFactory.getAnyInstance(), diskStoreName)) {
            return (CliStrings.format(
                CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0,
                new Object[] {diskStoreName}));
          }
          tableDescriptor.setDiskStore(diskStoreName);
        }
      }
      ResultCollector<?, ?> rc = CliUtil.executeFunction(new CreateMTableFunction(),
          new Object[] {mTableName, tableDescriptor}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
      boolean accumulatedData = false;
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null || result.getMessage() != null) {
          accumulatedData = true;
        }
      }
      if (!accumulatedData) {
        return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
            (CommandResult) ResultBuilder.createGemFireErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__ALREADY_EXISTS_ERROR))));
      }
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (NullPointerException e) {
      e.printStackTrace();
      return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
          (CommandResult) ResultBuilder.createGemFireErrorResult(
              CliStrings.format(MashCliStrings.CREATE_MTABLE__DISKSTORE__ERROR))));
    } catch (Exception e) {
      return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
          (CommandResult) ResultBuilder.createGemFireErrorResult(
              CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0))));

    } catch (Throwable th) {
      return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
          (CommandResult) ResultBuilder.createGemFireErrorResult(
              CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0))));
    }
    return (CommandResponseBuilder.createCommandResponseJson(MashCliStrings.CREATE_MTABLE,
        (CommandResult) ResultBuilder
            .createInfoResult(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS))));
  }

  private boolean diskStoreExists(MCache cache, String diskStoreName) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();

    Set<Map.Entry<String, String[]>> entrySet = diskstore.entrySet();

    for (Map.Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (CliUtil.contains(value, diskStoreName)) {
        return true;
      }
    }

    return false;
  }
}
