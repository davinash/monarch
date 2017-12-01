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

package org.apache.geode.internal.cache;

/**
 * Function used by the 'MTable data commands' mash command to mget mput and mscan on each member.
 * 
 * @since 8.0
 */

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.RegionDataOrder;
import io.ampool.management.internal.cli.domain.MTableDataCommandRequest;
import io.ampool.management.internal.cli.domain.MTableDataCommandResult;
import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.DataCommands;
import org.apache.geode.management.internal.cli.commands.MTableDataCommands;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.multistep.CLIMultiStepHelper;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableDataCommandsFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;


  public MTableDataCommandsFunction() {
    super();
  }

  private boolean checkTableExists(String tableName) {
    final String mTableName = tableName.startsWith("/") ? tableName.substring(1) : tableName;
    return MCacheFactory.getAnyInstance().getAdmin().tableExists(mTableName);
  }

  @Override
  public void execute(FunctionContext context) {
    final MTableDataCommandRequest mTableDataCommandRequest =
        (MTableDataCommandRequest) context.getArguments();
    final String mTableOpType = mTableDataCommandRequest.getCommand();
    if (checkTableExists(mTableDataCommandRequest.getMTableName())) {
      MTableDataCommandResult mTableDataCommandResult = null;
      if (mTableOpType.equals(MashCliStrings.MPUT)) {
        mTableDataCommandResult = put(mTableDataCommandRequest);
      }
      if (mTableOpType.equals(MashCliStrings.MGET)) {
        mTableDataCommandResult = get(mTableDataCommandRequest);
      }
      if (mTableOpType.equals(MashCliStrings.MSCAN)) {
        mTableDataCommandResult = scan(mTableDataCommandRequest);
      }
      if (mTableOpType.equals(MashCliStrings.MDELETE)) {
        mTableDataCommandResult = delete(mTableDataCommandRequest);
      }
      if (mTableOpType.equals(MashCliStrings.APPEND)) {
        mTableDataCommandResult = append(mTableDataCommandRequest);
      }
      context.getResultSender().lastResult(mTableDataCommandResult);
    } else {
      Result userErrorResult = ResultBuilder.createUserErrorResult(CliStrings
          .format(MashCliStrings.MTABLE_NOT_FOUND, mTableDataCommandRequest.getMTableName()));
      context.getResultSender().lastResult(userErrorResult);
    }

  }

  public MTableDataCommandResult put(MTableDataCommandRequest mTableDataCommandRequest) {
    try {
      final String mTableName = mTableDataCommandRequest.getMTableName().startsWith("/")
          ? mTableDataCommandRequest.getMTableName().substring(1)
          : mTableDataCommandRequest.getMTableName();
      final MTable table = MCacheFactory.getAnyInstance().getTable(mTableName);
      Put record = new Put(Bytes.toBytes(mTableDataCommandRequest.getKey()));
      final String valueJson;
      Map<String, String> strBinaryColumns = new HashMap<>();
      if (mTableDataCommandRequest.getValue() != null) {
        logger.debug("Creating row using value= {}", mTableDataCommandRequest.getValue());
        String[] tmp = mTableDataCommandRequest.getValue().split(",");
        JSONObject jsonObject = new JSONObject();
        Map<ByteArrayKey, MColumnDescriptor> mapColType =
            table.getTableDescriptor().getColumnsByName();
        ByteArrayKey keyBytes = new ByteArrayKey();
        for (String v : tmp) {
          String[] t = StringUtils.split(v, "=");
          keyBytes.setData(Bytes.toBytes(t[0]));
          if (mapColType.get(keyBytes).getColumnType() != BasicTypes.BINARY) {
            jsonObject.put(t[0], t[1]);
          } else {
            strBinaryColumns.put(t[0], t[1]);
          }
        }
        valueJson = jsonObject.toString();
      } else {
        valueJson = mTableDataCommandRequest.getValueJson();
        logger.debug("Creating row using valueJson= {}", valueJson);
      }
      Map<String, Object> valueMap;
      try {
        valueMap = TypeUtils.getValueMap(table.getTableDescriptor(), valueJson);
      } catch (Exception e) {
        final String msg = "Error processing Row: " + e.getMessage();
        return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MPUT, null,
            null, null, null, e, msg, false);
      }
      for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
        record.addColumn(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, String> entry : strBinaryColumns.entrySet()) {
        record.addColumn(entry.getKey(), Bytes.toBytes(strBinaryColumns.get(entry.getKey())));
      }

      record.setTimeStamp(mTableDataCommandRequest.getTimeStamp());
      table.put(record);
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MPUT,
          mTableDataCommandRequest.getMTableName(), null, null, null, null,
          "Successfully put the entry in MTable", true);
    } catch (Exception ex) {
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MPUT,
          mTableDataCommandRequest.getMTableName(), null, null, null, ex, ex.getMessage(), false);
    }
  }


  public MTableDataCommandResult append(MTableDataCommandRequest mTableDataCommandRequest) {
    try {
      final String mTableName = mTableDataCommandRequest.getMTableName().startsWith("/")
          ? mTableDataCommandRequest.getMTableName().substring(1)
          : mTableDataCommandRequest.getMTableName();
      final FTable table = MCacheFactory.getAnyInstance().getFTable(mTableName);
      final String valueJson;
      Map<String, String> strBinaryColumns = new HashMap<>();
      valueJson = mTableDataCommandRequest.getValueJson();
      Map<String, Object> valueMap;
      try {
        valueMap = TypeUtils.getValueMap(table.getTableDescriptor(), valueJson, table);
      } catch (Exception e) {
        final String msg = "Error processing Row: " + e.getMessage();
        return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MPUT, null,
            null, null, null, e, msg, false);
      }
      Record record = new Record();
      for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
        record.add(entry.getKey(), entry.getValue());
      }
      table.append(record);
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.APPEND,
          mTableDataCommandRequest.getMTableName(), null, null, null, null,
          "Successfully appended the entry in MTable", true);
    } catch (Exception ex) {
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.APPEND,
          mTableDataCommandRequest.getMTableName(), null, null, null, ex, ex.getMessage(), false);
    }

  }


  public MTableDataCommandResult delete(MTableDataCommandRequest mTableDataCommandRequest) {
    try {
      final String mTableName = mTableDataCommandRequest.getMTableName().startsWith("/")
          ? mTableDataCommandRequest.getMTableName().substring(1)
          : mTableDataCommandRequest.getMTableName();
      Delete delete = new Delete(Bytes.toBytes(mTableDataCommandRequest.getKey()));
      String[] columnsList = new String[0];
      if (mTableDataCommandRequest.getValue() != null) {
        columnsList = mTableDataCommandRequest.getValue().split(",");
        for (String column : columnsList) {
          delete.addColumn(Bytes.toBytes(column));
        }

      }
      final MTable table = MCacheFactory.getAnyInstance().getTable(mTableName);
      if (mTableDataCommandRequest.getTimeStamp() != 0)
        delete.setTimestamp(mTableDataCommandRequest.getTimeStamp());
      table.delete(delete);
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MDELETE,
          mTableDataCommandRequest.getMTableName(), null, null, null, null,
          "Successfully deleted the entry from MTable", true);
    } catch (Exception ex) {
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MDELETE,
          mTableDataCommandRequest.getMTableName(), null, null, null, ex, ex.getMessage(), false);
    }
  }


  public MTableDataCommandResult get(MTableDataCommandRequest mTableDataCommandRequest) {
    try {
      final String mTableName = mTableDataCommandRequest.getMTableName().startsWith("/")
          ? mTableDataCommandRequest.getMTableName().substring(1)
          : mTableDataCommandRequest.getMTableName();
      final MTable table = MCacheFactory.getAnyInstance().getTable(mTableName);
      List<Row> mTableValues = new ArrayList<>();
      Get get = new Get(Bytes.toBytes(mTableDataCommandRequest.getKey()));
      if (mTableDataCommandRequest.getTimeStamp() != 0)
        get.setTimeStamp(mTableDataCommandRequest.getTimeStamp());
      Row result = table.get(get);
      mTableValues.add(result);
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MGET,
          mTableDataCommandRequest.getMTableName(), null, null, mTableValues, null, null, true);

    } catch (Exception ex) {
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MGET,
          mTableDataCommandRequest.getMTableName(), null, null, null, ex, ex.getMessage(), false);
    }
  }

  public MTableDataCommandResult scan(MTableDataCommandRequest request) {
    List<Row> mTableValues = new ArrayList<>();
    Scan scan = null;
    final String mTableName = request.getMTableName().startsWith("/")
        ? request.getMTableName().substring(1) : request.getMTableName();
    final byte[] startRow = request.getStartKey();
    final byte[] stopRow = request.getEndKey();
    MonarchCacheImpl instance = (MonarchCacheImpl) MCacheFactory.getAnyInstance();
    InternalTable tbl = (InternalTable) instance.getAnyTable(mTableName);
    if (startRow != null || stopRow != null) {

      RegionDataOrder rdo = ((AmpoolTableRegionAttributes) tbl.getInternalRegion().getAttributes()
          .getCustomAttributes()).getRegionDataOrder();
      if (rdo.getId() == 2 || rdo.getId() == 3) {
        return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MSCAN,
            request.getMTableName(), startRow, stopRow, null, null,
            CliStrings.format(MashCliStrings.MSCAN__MSG__TYPE_NOT,
                MashCliStrings.MSCAN__START_KEY + "/" + MashCliStrings.MSCAN__END_KEY),
            false, tbl instanceof FTable);
      }
    }
    if (tbl instanceof MTable) {
      try {
        final MTable table = (MTable) tbl;
        if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          if (startRow != null && stopRow != null) {
            scan = new Scan(startRow, stopRow);
          } else {
            scan = new Scan();
            if (startRow != null) {
              scan.setStartRow(startRow);
            }
            if (stopRow != null) {
              scan.setStopRow(stopRow);
            }
          }
          scan.setMaxResultLimit(request.getMaxLimit());
          Scanner resultScanner = table.getScanner(scan);
          Iterator<Row> resultIterator = resultScanner.iterator();
          while (resultIterator.hasNext()) {
            Row result = resultIterator.next();
            if (result != null) {
              mTableValues.add(result);
            }
          }
          return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MSCAN,
              request.getMTableName(), startRow, stopRow, mTableValues, null, null, true,
              tbl instanceof FTable);
        } else {
          scan = new Scan();
          scan.setMaxResultLimit(request.getMaxLimit());
          Scanner resultScanner = table.getScanner(scan);
          Iterator<Row> resultIterator = resultScanner.iterator();
          while (resultIterator.hasNext()) {
            Row result = resultIterator.next();
            if (result != null) {
              mTableValues.add(result);
            }
          }
          return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MSCAN,
              request.getMTableName(), null, null, mTableValues, null, null, true,
              tbl instanceof FTable);
        }
      } catch (Exception ex) {

        return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MSCAN,
            request.getMTableName(), startRow, stopRow, null, ex, MashCliStrings.MSCAN__EXCCEPTION,
            false, tbl instanceof FTable);
      }
    } else if (tbl instanceof FTable) {
      Scan iscan = new Scan();
      iscan.setMaxResultLimit(request.getMaxLimit());
      final Scanner scanner = tbl.getScanner(iscan);
      while (true) {
        Row record = scanner.next();
        if (record != null) {
          mTableValues.add(record);
        } else {
          break;
        }
      }
      return MTableDataCommandResult.createMTableDataCommandResult(MashCliStrings.MSCAN,
          request.getMTableName(), startRow, stopRow, mTableValues, null, null, true,
          tbl instanceof FTable);
    } else {
      return null;
    }
  }

  @Override
  public String getId() {
    return MTableDataCommandsFunction.class.getName();
  }

  protected static final String SCAN_STEP_DISPLAY = "SCAN_DISPLAY";
  protected static final String SCAN_STEP_MOVE = "SCAN_PAGE_MOVE";
  protected static final String SCAN_STEP_END = "SCAN_END";
  protected static final String SCAN_STEP_EXEC = "SCAN_EXEC";

  public static final String SCAN_PAGE_START = "startCount";
  public static final String SCAN_PAGE_END = "endCount";
  public static final String SCAN_TRACE = "Query Trace";

  private static MTableDataCommandResult cachedResult = null;

  public static class ScanDisplayStep extends CLIMultiStepHelper.LocalStep {

    public ScanDisplayStep(Object[] arguments) {
      super(SCAN_STEP_DISPLAY, arguments);
    }

    @Override
    public Result exec() {
      String filePath = (String) commandArguments[7];
      boolean toFile = filePath != null && !filePath.isEmpty();
      boolean interactive = (Boolean) commandArguments[4];
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();
      int startCount = args.getInt(SCAN_PAGE_START);
      int endCount = args.getInt(SCAN_PAGE_END);
      int rows = args.getInt(MTableDataCommandResult.NUM_ROWS); // returns Zero if no rows added so
                                                                // it works.
      boolean flag = args.getBoolean(MTableDataCommandResult.RESULT_FLAG);
      CommandResult commandResult = CLIMultiStepHelper.getDisplayResultFromArgs(args);
      if (!toFile) {
        Gfsh.println();
        while (commandResult.hasNextLine()) {
          Gfsh.println(commandResult.nextLine());
        }
      }
      if (flag) {
        boolean paginationNeeded = (startCount < rows) && (endCount < rows) && interactive && flag;
        if (paginationNeeded) {
          while (true) {
            String message = ("Press n to move to next page, q to quit and p to previous page : ");
            try {
              String step = Gfsh.getCurrentInstance().interact(message);
              if ("n".equals(step)) {
                int nextStart = startCount + DataCommandFunction.getPageSize() + 1;
                return CLIMultiStepHelper.createBannerResult(
                    new String[] {SCAN_PAGE_START, SCAN_PAGE_END,},
                    new Object[] {nextStart, (nextStart + DataCommandFunction.getPageSize())},
                    SCAN_STEP_MOVE);
              } else if ("p".equals(step)) {
                int nextStart = startCount - DataCommandFunction.getPageSize();
                if (nextStart < 0) {
                  nextStart = 0;
                }
                return CLIMultiStepHelper.createBannerResult(
                    new String[] {SCAN_PAGE_START, SCAN_PAGE_END},
                    new Object[] {nextStart, (nextStart + DataCommandFunction.getPageSize())},
                    SCAN_STEP_MOVE);
              } else if ("q".equals(step)) {
                return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {},
                    SCAN_STEP_END);
              } else {
                Gfsh.println("Unknown option ");
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
      return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {}, SCAN_STEP_END);
    }
  }


  public static class ScanMoveStep extends CLIMultiStepHelper.RemoteStep {

    private static final long serialVersionUID = 1L;

    public ScanMoveStep(Object[] arguments) {
      super(SCAN_STEP_MOVE, arguments);
    }

    @Override
    public Result exec() {
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();
      int startCount = args.getInt(SCAN_PAGE_START);
      int endCount = args.getInt(SCAN_PAGE_END);
      return cachedResult.pageResult(startCount, endCount, SCAN_STEP_DISPLAY);
    }
  };

  /**
   * Return the byte-array for the provided key using specified key-type.
   *
   * @param keyType the key-type (either STRING or BINARY)
   * @param key the input key
   * @return byte-array corresponding to the input key
   */
  public static byte[] convertKeyToBytes(final String keyType, final String key) {
    if (key == null || key.isEmpty()) {
      return null;
    }
    byte[] ret;
    switch (keyType) {
      case "BINARY":
        final String[] bytes = key.split(",");
        ret = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
          ret[i] = Byte.valueOf(bytes[i]);
        }
        break;
      case "STRING": {
        ret = key.getBytes(Charset.defaultCharset());
        break;
      }
      default:
        throw new IllegalArgumentException(MashCliStrings.MSCAN__MSG_INVALID_KEY_TYPE);
    }
    return ret;
  }

  public static class ScanExecStep extends CLIMultiStepHelper.RemoteStep {

    private static final long serialVersionUID = 1L;

    private static SecurityService securityService = SecurityService.getSecurityService();

    public ScanExecStep(Object[] arguments) {
      super(SCAN_STEP_EXEC, arguments);
    }

    @Override
    public Result exec() {
      String mtableName = (String) commandArguments[0];
      String startKey = (String) commandArguments[1];
      String stopKey = (String) commandArguments[2];
      boolean interactive = (Boolean) commandArguments[4];
      String keyType = (String) commandArguments[5];
      int maxLimit = (int) commandArguments[6];
      String filePath = (String) commandArguments[7];
      MTableDataCommandResult result = _scan(mtableName, startKey, stopKey, keyType, maxLimit);
      int endCount = 0;
      cachedResult = result;
      if (interactive) {
        endCount = DataCommandFunction.getPageSize();
      } else {
        if (result.getValues() != null) {
          endCount = result.getValues().size();
        }
      }
      if (interactive) {
        return result.pageResult(0, endCount, SCAN_STEP_DISPLAY);
      } else {
        if (filePath != null && !filePath.isEmpty()) {
          int startCount = 0;

          // check for file extension to be .csv
          if (!filePath.endsWith(MashCliStrings.MTABLE_DATA_FILE_EXTENSION)) {
            return ResultBuilder.createUserErrorResult(MashCliStrings.format(
                MashCliStrings.INVALID_FILE_EXTENTION, MashCliStrings.MTABLE_DATA_FILE_EXTENSION));
          }

          // check if file already exists
          File locatedFile = new File(filePath);
          if (locatedFile.exists() && !locatedFile.isDirectory()) {
            return ResultBuilder
                .createUserErrorResult(MashCliStrings.format(MashCliStrings.FILE_EXISTS, filePath));
          }

          // write to file
          try {
            result.writeToFile(startCount, endCount, filePath);
          } catch (IOException ex) {
            return ResultBuilder.createUserErrorResult(MashCliStrings.format(ex.getMessage()));
          }
        }
        return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {},
            SCAN_STEP_END);
      }
    }

    public MTableDataCommandResult _scan(String mtableName, String startKey, String stopKey,
        final String keyType, final int maxLimit) {
      Cache cache = CacheFactory.getAnyInstance();
      MTableDataCommandResult dataResult = null;
      byte[] startKeyBytes = null;
      byte[] stopKeyBytes = null;
      Throwable e = null;
      try {
        startKeyBytes = convertKeyToBytes(keyType, startKey);
        stopKeyBytes = convertKeyToBytes(keyType, stopKey);
      } catch (NumberFormatException nfe) {
        e = nfe.getCause();
      } catch (IllegalArgumentException iae) {
        return MTableDataCommandResult.createScanResult(mtableName, startKey, stopKey, null,
            MashCliStrings.MSCAN__MSG_INVALID_KEY_BINARY, false);
      }

      /** handle invalid options.. **/
      if (mtableName == null || mtableName.isEmpty()) {
        return MTableDataCommandResult.createScanResult(mtableName, startKey, stopKey, null,
            CliStrings.GET__MSG__REGIONNAME_EMPTY, false);
      }
      final String msg = "BINARY".equals(keyType) ? MashCliStrings.MSCAN__MSG_INVALID_KEY_BINARY
          : MashCliStrings.MSCAN__MSG_INVALID_KEY;
      if (startKey != null && startKeyBytes == null) {
        return MTableDataCommandResult.createScanResult(mtableName, startKey, stopKey, null,
            MashCliStrings.format(msg, MashCliStrings.MSCAN__START_KEY), false);
      }
      if (stopKey != null && stopKeyBytes == null) {
        return MTableDataCommandResult.createScanResult(mtableName, startKey, stopKey, null,
            MashCliStrings.format(msg, MashCliStrings.MSCAN__END_KEY), false);
      }

      try {
        // authorize data read on these regions
        this.securityService.authorizeRegionRead(mtableName);

        Set<DistributedMember> members =
            DataCommands.getRegionAssociatedMembers(mtableName, cache, true);
        if (members != null && members.size() > 0) {
          MTableDataCommandsFunction function = new MTableDataCommandsFunction();
          MTableDataCommandRequest request = new MTableDataCommandRequest();
          request.setCommand(MashCliStrings.MSCAN);
          request.setStartKey(startKeyBytes);
          request.setEndKey(stopKeyBytes);
          request.setMTableName(mtableName);
          request.setMaxLimit(maxLimit);
          Subject subject = this.securityService.getSubject();
          if (subject != null) {
            request.setPrincipal((Serializable) subject.getPrincipal());
          }
          dataResult = MTableDataCommands.callFunctionForMTable(request, function, members);
          return dataResult;
        } else {
          return MTableDataCommandResult.createInfoMTableDataCommandResult(null, null, null, null,
              null, null, CliStrings.format(MashCliStrings.MTABLE_NOT_FOUND, mtableName), false);
        }
      } catch (AuthenticationRequiredException | AuthenticationFailedException
          | NotAuthorizedException ex) {
        logger.info("Cought security exception: " + ex.getMessage());
        throw ex;
      } catch (Exception ex) {
        return MTableDataCommandResult.createInfoMTableDataCommandResult(null, null, null, null,
            null, ex, ex.getMessage(), false);
      }
    }
  };

  public static class ScanQuitStep extends CLIMultiStepHelper.RemoteStep {

    public ScanQuitStep(Object[] arguments) {
      super(SCAN_STEP_END, arguments);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public Result exec() {
      boolean interactive = (Boolean) commandArguments[4];
      String filePath = (String) commandArguments[7];
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();
      MTableDataCommandResult dataResult = cachedResult;
      cachedResult = null;
      if (interactive) {
        return CLIMultiStepHelper.createEmptyResult("END");
      } else {
        CompositeResultData rd = dataResult.toScanCommandResult(filePath);
        CompositeResultData.SectionResultData section =
            rd.addSection(CLIMultiStepHelper.STEP_SECTION);
        section.addData(CLIMultiStepHelper.NEXT_STEP_NAME, "END");
        return ResultBuilder.buildResult(rd);
      }
    }
  };
}
