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

package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.VMCachedDeserializable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * MCoprocessorUtils
 *
 * @since 0.2.0.0
 */

public final class MCoprocessorUtils {

  public static Object createInstance(String className) {
    Object instance = null;
    try {
      instance = ClassPathLoader.getLatest().forName(className).newInstance();
    } catch (InstantiationException e) {
      MCoprocessorUtils.getLogger()
          .info("MCoprocessorUtils::createInstance() InstantiationException caught!");
      throw new MCoprocessorException("Class Instantiation failed for classname " + className);
    } catch (IllegalAccessException e) {
      MCoprocessorUtils.getLogger()
          .info("MCoprocessorUtils::createInstance() IllegalAccessException caught!");
      throw new MCoprocessorException(
          "Illegal Access found while Instantiating classname " + className);
    } catch (ClassNotFoundException e) {
      MCoprocessorUtils.getLogger()
          .info("MCoprocessorUtils::createInstance() ClassNotFoundException caught!");
      throw new MCoprocessorException("classname: " + className + "not found");
    }
    return instance;
  }

  public static LogWriter getLogger() {
    return InternalDistributedSystem.getAnyInstance().getLogWriter();
  }

  public static MObserverContextImpl getMObserverContext(String regionName) {
    MTable table = MCacheFactory.getAnyInstance().getTable(regionName);
    return new MObserverContextImpl(table);
  }

  public static MObserverContextImpl getMObserverContextWithEvent(String regionName,
      EntryEvent event) {
    MTable table = MCacheFactory.getAnyInstance().getTable(regionName);
    return new MObserverContextImpl(table, event);
    // TODO set table region
  }

  public static Delete constructMDelete(byte[] rowKey, List<Integer> colPositions,
      String tableName) {
    Delete del = null;
    if (colPositions.size() == 0) {
      // complete row key delete
      del = new Delete(rowKey);
    } else {
      del = new Delete(rowKey);

      MTableDescriptor tableDescriptor =
          MCacheFactory.getAnyInstance().getMTableDescriptor(tableName);
      Map<MColumnDescriptor, Integer> colDescriptors = tableDescriptor.getColumnDescriptorsMap();
      Map<Integer, MColumnDescriptor> colPosToNameMap = reverseMap(colDescriptors);
      for (int colIndex = 0; colIndex < colPositions.size(); colIndex++) {
        MColumnDescriptor mColumnDescriptor = colPosToNameMap.get(colIndex);
        byte[] colName = mColumnDescriptor.getColumnName();
        del.addColumn(colName);
      }
    }
    return del;
  }

  private static Map<Integer, MColumnDescriptor> reverseMap(
      Map<MColumnDescriptor, Integer> colDescriptors) {
    Map<Integer, MColumnDescriptor> map = new HashMap<>();
    colDescriptors.forEach((k, v) -> {
      map.put(v, k);
    });
    return map;
  }

  public static Get constructMGet(byte[] rowKey, List<Integer> colPositions, String tableName) {
    Get get = null;
    if (colPositions.size() == 0) {
      // complete row key delete
      get = new Get(rowKey);
    } else {
      get = new Get(rowKey);
      MTableDescriptor tableDescriptor =
          MCacheFactory.getAnyInstance().getMTableDescriptor(tableName);
      Map<MColumnDescriptor, Integer> colDescriptors = tableDescriptor.getColumnDescriptorsMap();
      Map<Integer, MColumnDescriptor> colPosToNameMap = reverseMap(colDescriptors);
      for (int colIndex = 0; colIndex < colPositions.size(); colIndex++) {
        MColumnDescriptor mColumnDescriptor = (MColumnDescriptor) colPosToNameMap.get(colIndex);
        byte[] colName = mColumnDescriptor.getColumnName();
        get.addColumn(colName);
      }
    }
    return get;
  }

  public static Put constructMPut(byte[] rowKey, List<Integer> columnPositions, Object value,
      String tableName, boolean hasTimeStamp) {

    MTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getMTableDescriptor(tableName);
    Put put = new Put(rowKey);

    byte[][] data = null;
    if (value instanceof MValue) {
      data = ((MValue) value).getData();
    } else if (value instanceof VMCachedDeserializable) {
      data = ((MValue) ((VMCachedDeserializable) value).getDeserializedForReading()).getData();
    }

    for (int i = 0; i < columnPositions.size(); i++) {
      MColumnDescriptor columnDescriptorByIndex =
          tableDescriptor.getSchema().getColumnDescriptorByIndex(columnPositions.get(i));
      put.addColumn(columnDescriptorByIndex.getColumnNameAsString(),
          columnDescriptorByIndex.getColumnType().deserialize(data[columnPositions.get(i)]));
    }
    return put;
  }
}
