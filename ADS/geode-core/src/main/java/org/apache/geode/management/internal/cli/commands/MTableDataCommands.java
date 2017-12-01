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
package org.apache.geode.management.internal.cli.commands;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.ampool.management.internal.cli.domain.MTableDataCommandRequest;
import io.ampool.management.internal.cli.domain.MTableDataCommandResult;
import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.types.TypeHelper;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.MTableDataCommandsFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.ExportDataFunction;
import org.apache.geode.management.internal.cli.functions.ImportDataFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.multistep.CLIMultiStepHelper;
import org.apache.geode.management.internal.cli.multistep.CLIStep;
import org.apache.geode.management.internal.cli.multistep.MultiStepCommand;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

public class MTableDataCommands implements CommandMarker {

  final int resultItemCount = 9;
  private final ExportDataFunction exportDataFunction = new ExportDataFunction();
  private final ImportDataFunction importDataFunction = new ImportDataFunction();
  private static final Logger logger = LogService.getLogger();

  private SecurityService securityService = IntegratedSecurityService.getSecurityService();

  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @CliMetaData(shellOnly = false,
      relatedTopic = {CliStrings.TOPIC_GEODE_DATA, MashCliStrings.TOPIC_MTABLE})
  @CliCommand(value = {MashCliStrings.MPUT}, help = MashCliStrings.MPUT__HELP)
  public Result put(
      @CliOption(key = {CliStrings.PUT__KEY}, mandatory = true,
          help = MashCliStrings.MPUT__KEY__HELP) String key,
      @CliOption(key = {CliStrings.PUT__VALUE},
          help = MashCliStrings.MPUT__VALUE__HELP) String value,
      @CliOption(key = {MashCliStrings.MPUT__VALUE_JSON},
          help = MashCliStrings.MPUT__VALUE_JSON__HELP) String valueJson,
      @CliOption(key = {MashCliStrings.MPUT__TIMESTAMP}, mandatory = false,
          unspecifiedDefaultValue = "0",
          help = MashCliStrings.MPUT__TIMESTAMP__HELP) Integer timestamp,
      @CliOption(key = {MashCliStrings.MPUT__MTABLENAME}, mandatory = true,
          help = MashCliStrings.MPUT__MTABLENAME__HELP,
          optionContext = ConverterHint.TABLE_PATH) String regionPath) {

    this.securityService.authorizeRegionWrite(regionPath);

    LogWrapper logWrapper = LogWrapper.getInstance();
    Cache cache = CacheFactory.getAnyInstance();
    DataCommandResult dataResult = null;
    logWrapper.fine("cache " + cache);
    if (regionPath == null || regionPath.isEmpty()) {
      return makePresentationResult(DataCommandResult.createPutResult(key, null, null,
          MashCliStrings.MPUT__MSG__MTABLENAME_EMPTY, false));
    }
    if (key == null || key.isEmpty())
      return makePresentationResult(dataResult = DataCommandResult.createPutResult(key, null, null,
          CliStrings.PUT__MSG__KEY_EMPTY, false));
    if ((value == null || value.isEmpty()) && (valueJson == null || valueJson.isEmpty()))
      return makePresentationResult(dataResult = DataCommandResult.createPutResult(value, null,
          null, MashCliStrings.MPUT__MSG___VALUE_EMPTY, false));
    Set<DistributedMember> targetMembers;
    String[] groups1 = null;
    try {
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }
      MTableDataCommandRequest mTableDataCommandRequest = new MTableDataCommandRequest();
      mTableDataCommandRequest.setCommand(MashCliStrings.MPUT);
      mTableDataCommandRequest.setMTableName(regionPath);
      mTableDataCommandRequest.setKey(key);
      mTableDataCommandRequest.setTimeStamp(timestamp);
      /**
       * Adding another option --value-json that takes column values as JSON and then converts them
       * to the respective Java types before inserting into table. For now, keeping both options
       * --value (string-binary) and --value-json but going ahead we can deprecate/remove --value.
       * If both are provided together, give an error.
       */
      if (value != null && valueJson != null) {
        return makePresentationResult(DataCommandResult.createPutResult(value, null, null,
            MashCliStrings.MPUT__VALUE_JSON__HELP_1, false));
      }
      mTableDataCommandRequest.setValueJson(valueJson);
      if (value != null)
        mTableDataCommandRequest.setValue(value);
      List<MTableDataCommandResult> mTableDataCommandResults =
          (List<MTableDataCommandResult>) CliUtil.executeFunction(new MTableDataCommandsFunction(),
              mTableDataCommandRequest, targetMembers).getResult();
      for (MTableDataCommandResult mTableDataCommandResult : mTableDataCommandResults) {
        if (!mTableDataCommandResult.isOperationCompletedSuccessfully())
          return ResultBuilder.createMTableErrorResult(mTableDataCommandResult.getErrorString());
      }
      return ResultBuilder.createInfoResult(MashCliStrings.MPUT__SUCCESS_MSG);
    } catch (Exception e) {
      return ResultBuilder.createMTableErrorResult(CliStrings.format(MashCliStrings.MPUT__ERROR_MSG,
          key, e.getClass() + " : " + e.getMessage()));
    }
  }


  @CliMetaData(shellOnly = false,
      relatedTopic = {CliStrings.TOPIC_GEODE_DATA, MashCliStrings.TOPIC_FTABLE})
  @CliCommand(value = {MashCliStrings.APPEND}, help = MashCliStrings.APPEND__HELP)
  public Result append(
      @CliOption(key = {MashCliStrings.APPEND__VALUE_JSON},
          help = MashCliStrings.APPEND__VALUE_JSON__HELP) String valueJson,
      @CliOption(key = {MashCliStrings.APPEND__MTABLENAME}, mandatory = true,
          help = MashCliStrings.APPEND__MTABLENAME__HELP,
          optionContext = ConverterHint.TABLE_PATH) String regionPath) {

    this.securityService.authorizeRegionWrite(regionPath);
    LogWrapper logWrapper = LogWrapper.getInstance();
    Cache cache = CacheFactory.getAnyInstance();
    DataCommandResult dataResult = null;
    logWrapper.fine("cache " + cache);
    if (regionPath == null || regionPath.isEmpty()) {
      return makePresentationResult(DataCommandResult.createPutResult(null, null, null,
          MashCliStrings.APPEND__MSG__MTABLENAME_EMPTY, false));
    }
    if ((valueJson == null || valueJson.isEmpty()))
      return makePresentationResult(dataResult = DataCommandResult.createPutResult(null, null, null,
          MashCliStrings.APPEND__ERROR_MSG, false));
    Set<DistributedMember> targetMembers;
    String[] groups1 = null;
    try {
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }
      MTableDataCommandRequest mTableDataCommandRequest = new MTableDataCommandRequest();
      mTableDataCommandRequest.setCommand(MashCliStrings.APPEND);
      mTableDataCommandRequest.setMTableName(regionPath);
      mTableDataCommandRequest.setValueJson(valueJson);
      List<MTableDataCommandResult> mTableDataCommandResults =
          (List<MTableDataCommandResult>) CliUtil.executeFunction(new MTableDataCommandsFunction(),
              mTableDataCommandRequest, targetMembers).getResult();
      for (MTableDataCommandResult mTableDataCommandResult : mTableDataCommandResults) {
        if (!mTableDataCommandResult.isOperationCompletedSuccessfully())
          return ResultBuilder.createMTableErrorResult(mTableDataCommandResult.getErrorString());
      }
      return ResultBuilder.createInfoResult(MashCliStrings.APPEND__SUCCESS_MSG);
    } catch (Exception e) {
      return ResultBuilder.createMTableErrorResult(CliStrings
          .format(MashCliStrings.APPEND__ERROR_MSG, null, e.getClass() + " : " + e.getMessage()));
    }
  }


  @CliMetaData(shellOnly = false, relatedTopic = {CliStrings.TOPIC_GEODE_DATA,
      MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @CliCommand(value = {MashCliStrings.MDELETE}, help = MashCliStrings.MDELETE__HELP)
  public Result delete(
      @CliOption(key = {MashCliStrings.MDELETE__KEY}, mandatory = true,
          help = MashCliStrings.MDELETE__KEY__HELP) String key,
      @CliOption(key = {MashCliStrings.MDELETE_COLUMN}, mandatory = false,
          help = MashCliStrings.MDELETE_COLUMN__HELP) String columns,
      @CliOption(key = {MashCliStrings.MDELETE__TIMESTAMP}, mandatory = false,
          unspecifiedDefaultValue = "0",
          help = MashCliStrings.MDELETE__TIMESTAMP__HELP) Integer timestamp,
      @CliOption(key = {MashCliStrings.MDELETE__MTABLENAME}, mandatory = true,
          help = MashCliStrings.MDELETE__MTABLENAME__HELP,

          optionContext = ConverterHint.TABLE_PATH) String regionPath) {

    this.securityService.authorizeRegionWrite(regionPath, key);
    LogWrapper logWrapper = LogWrapper.getInstance();
    Cache cache = CacheFactory.getAnyInstance();
    DataCommandResult dataResult = null;
    logWrapper.fine("cache " + cache);
    if (regionPath == null || regionPath.isEmpty()) {
      return makePresentationResult(DataCommandResult.createPutResult(key, null, null,
          MashCliStrings.MDELETE__MSG__MTABLENAME_EMPTY, false));
    }
    if (key == null || key.isEmpty())
      return makePresentationResult(dataResult = DataCommandResult.createPutResult(key, null, null,
          CliStrings.PUT__MSG__KEY_EMPTY, false));

    Set<DistributedMember> targetMembers;
    String[] groups1 = null;
    try {
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }
      MTableDataCommandRequest mTableDataCommandRequest = new MTableDataCommandRequest();
      mTableDataCommandRequest.setCommand(MashCliStrings.MDELETE);
      mTableDataCommandRequest.setMTableName(regionPath);
      mTableDataCommandRequest.setKey(key);
      mTableDataCommandRequest.setValue(columns);
      mTableDataCommandRequest.setTimeStamp(timestamp);

      List<MTableDataCommandResult> result =
          (List<MTableDataCommandResult>) CliUtil.executeFunction(new MTableDataCommandsFunction(),
              mTableDataCommandRequest, targetMembers).getResult();

      for (MTableDataCommandResult mTableDataCommandResult : result) {
        if (!mTableDataCommandResult.isOperationCompletedSuccessfully())
          return ResultBuilder.createMTableErrorResult(mTableDataCommandResult.getErrorString());
      }
      return ResultBuilder.createInfoResult("MDelete completed successfully");
    } catch (Exception e) {
      return ResultBuilder.createMTableErrorResult(CliStrings
          .format(MashCliStrings.MDELETE__ERROR_MSG, key, e.getClass() + " : " + e.getMessage()));
    }
  }

  private Result makePresentationResult(DataCommandResult dataResult) {
    if (dataResult != null)
      return dataResult.toCommandResult();
    else
      return ResultBuilder.createMTableErrorResult("Error executing data command");
  }

  @CliMetaData(shellOnly = false,
      relatedTopic = {CliStrings.TOPIC_GEODE_DATA, MashCliStrings.TOPIC_MTABLE})
  @CliCommand(value = {MashCliStrings.MGET}, help = MashCliStrings.MGET__HELP)
  public Result get(
      @CliOption(key = {CliStrings.GET__KEY}, mandatory = true,
          help = MashCliStrings.MGET__KEY__HELP) String key,
      @CliOption(key = {MashCliStrings.MGET__MTABLENAME}, mandatory = true,
          help = MashCliStrings.MGET__MTABLENAME__HELP,
          optionContext = ConverterHint.TABLE_PATH) String regionPath,
      @CliOption(key = {MashCliStrings.MGET__TIMESTAMP}, mandatory = false,
          unspecifiedDefaultValue = "0",
          help = MashCliStrings.MGET__TIMESTAMP__HELP) Integer timestamp) {

    this.securityService.authorizeRegionRead(regionPath, key);
    DataCommandResult dataResult = null;
    if (regionPath == null || regionPath.isEmpty()) {
      return makePresentationResult(dataResult = DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__REGIONNAME_EMPTY, false));
    }

    if (key == null || key.isEmpty()) {
      return makePresentationResult(dataResult = DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__KEY_EMPTY, false));
    }

    Set<DistributedMember> targetMembers;
    String[] groups1 = null;
    try {
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }
      MTableDataCommandRequest mTableDataCommandRequest = new MTableDataCommandRequest();
      mTableDataCommandRequest.setCommand(MashCliStrings.MGET);
      mTableDataCommandRequest.setMTableName(regionPath);
      mTableDataCommandRequest.setKey(key);
      mTableDataCommandRequest.setTimeStamp(timestamp);

      Subject subject = this.securityService.getSubject();
      if (subject != null) {
        mTableDataCommandRequest.setPrincipal((Serializable) subject.getPrincipal());
      }

      List<MTableDataCommandResult> result =
          (List<MTableDataCommandResult>) CliUtil.executeFunction(new MTableDataCommandsFunction(),
              mTableDataCommandRequest, targetMembers).getResult();
      return (toMTableResult(key, result));
    } catch (Exception e) {
      return ResultBuilder.createMTableErrorResult(CliStrings.format(MashCliStrings.MGET__ERROR_MSG,
          key, e.getClass() + " : " + e.getMessage()));
    }
  }

  @CliMetaData(shellOnly = false, relatedTopic = {CliStrings.TOPIC_GEODE_DATA,
      MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @CliCommand(value = {MashCliStrings.MSCAN}, help = MashCliStrings.MSCAN__HELP)
  @MultiStepCommand
  public Object scan(
      @CliOption(key = {MashCliStrings.MSCAN__START_KEY},
          help = MashCliStrings.MSCAN__START_KEY_HELP) String startKey,
      @CliOption(key = {MashCliStrings.MSCAN__END_KEY},
          help = MashCliStrings.MSCAN__END_KEY_HELP) String stopKey,
      @CliOption(key = MashCliStrings.MSCAN__KEY_TYPE,
          help = MashCliStrings.MSCAN__MSG_KEY_TYPE_HELP,
          unspecifiedDefaultValue = "STRING") String keyType,
      @CliOption(key = {MashCliStrings.MSCAN__MAX_LIMIT},
          help = MashCliStrings.MSCAN__MSG_MAX_LIMIT_HELP,
          unspecifiedDefaultValue = "1000") int maxlimit,
      @CliOption(key = CliStrings.QUERY__STEPNAME, mandatory = false, help = "Step name",
          unspecifiedDefaultValue = CliStrings.QUERY__STEPNAME__DEFAULTVALUE) String stepName,
      @CliOption(key = CliStrings.QUERY__INTERACTIVE, mandatory = false,
          help = CliStrings.QUERY__INTERACTIVE__HELP,
          unspecifiedDefaultValue = "true") boolean interactive,
      @CliOption(key = {MashCliStrings.MSCAN__MTABLENAME}, mandatory = true,
          help = MashCliStrings.MSCAN__MTABLENAME__HELP,
          optionContext = ConverterHint.TABLE_PATH) String mtableName,
      @CliOption(key = MashCliStrings.EXPORT_DATA__FILE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, mandatory = false,
          help = MashCliStrings.EXPORT_DATA__FILE__HELP) String filePath) {

    if (filePath != null && !filePath.isEmpty()) {
      interactive = false;
    }

    Object[] arguments = new Object[] {mtableName, startKey, stopKey, stepName, interactive,
        keyType, maxlimit, filePath};
    CLIStep exec = new MTableDataCommandsFunction.ScanExecStep(arguments);
    CLIStep display = new MTableDataCommandsFunction.ScanDisplayStep(arguments);
    CLIStep move = new MTableDataCommandsFunction.ScanMoveStep(arguments);
    CLIStep quit = new MTableDataCommandsFunction.ScanQuitStep(arguments);
    CLIStep[] steps = {exec, display, move, quit};
    return CLIMultiStepHelper.chooseStep(steps, stepName);

  }

  @SuppressWarnings("rawtypes")
  public static MTableDataCommandResult callFunctionForMTable(MTableDataCommandRequest request,
      MTableDataCommandsFunction mTableDataCommandsFunction, Set<DistributedMember> members) {

    Iterator<DistributedMember> distributedMemberIterator = members.iterator();
    MTableDataCommandResult result = null;
    int totalLimits = request.getMaxLimit();
    while (distributedMemberIterator.hasNext()) {
      if (result != null) {
        request.setMaxLimit(totalLimits - result.getValues().size());
      }
      ResultCollector collector = FunctionService.onMember(distributedMemberIterator.next())
          .withArgs(request).execute(mTableDataCommandsFunction);
      List list = (List) collector.getResult();
      Object object = list.get(0);
      if (object instanceof Throwable) {
        Throwable error = (Throwable) object;
        result = new MTableDataCommandResult();
        result.setErorr(error);
        result.setErrorString(error.getMessage());
        return result;
      }

      if (!((MTableDataCommandResult) object).isOperationCompletedSuccessfully()) {
        return (MTableDataCommandResult) object;
      }

      if (result == null) {
        result = (MTableDataCommandResult) object;
      } else {
        result.aggregate((MTableDataCommandResult) object);
      }
      if (result.getValues().size() >= totalLimits) {
        break;
      }
    }
    return result;
  }

  protected Result toMTableResult(String key, final List<MTableDataCommandResult> result) {
    final CompositeResultData MTableData = ResultBuilder.createCompositeResultData();
    boolean accumulatedData = false;

    Iterator<MTableDataCommandResult> it = result.iterator();
    while (it.hasNext()) {
      MTableDataCommandResult innerResults = it.next();
      if (innerResults != null && innerResults.isOperationCompletedSuccessfully()) {
        for (Object innerResult : innerResults.getValues()) {
          final Row next = (Row) innerResult;
          if (next == null)
            continue;
          final CompositeResultData.SectionResultData mResultData = MTableData.addSection();
          mResultData.addData("KEY", Bytes.toString(((Row) innerResult).getRowId()));
          for (Cell cell : next.getCells()) {
            mResultData.addData(Bytes.toString(cell.getColumnName()),
                TypeHelper.deepToString(cell.getColumnValue()));
            accumulatedData = true;
          }
          if (accumulatedData)
            mResultData.addSeparator('-');

        }
      } else {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(MashCliStrings.MTABLE_NOT_FOUND, innerResults.getmTableName()));
      }
    }
    if (!accumulatedData) {
      return ResultBuilder
          .createUserErrorResult(CliStrings.format(MashCliStrings.MGET__ERROR_NOT_FOUND, key));
    }
    return ResultBuilder.buildResult(MTableData);
  }

  @CliAvailabilityIndicator({MashCliStrings.MGET, MashCliStrings.MPUT, MashCliStrings.MSCAN,
      MashCliStrings.MDELETE})
  public boolean isMtableDataCommandsAvailable() {
    return (!CliUtil.isGfshVM() || (getGfsh() != null && getGfsh().isConnectedAndReady()));
  }
}
