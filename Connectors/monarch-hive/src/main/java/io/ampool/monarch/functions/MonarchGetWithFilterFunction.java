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

package io.ampool.monarch.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.internal.MResultParser;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.partition.PartitionRegionHelper;

/**
 * Created on: 2016-01-14
 * Since version: 0.2.0
 */
public class MonarchGetWithFilterFunction implements Declarable, Function {
  @Override
  public void init(Properties properties) {
    //// initialization...
  }

  @Override
  public void execute(final FunctionContext functionContext) {
    Object[] args = (Object[]) functionContext.getArguments();
    final String tableName = (String) args[0];
    final MPredicateHolder[] ph = args.length > 1 && args[1] != null ? (MPredicateHolder[])args[1] : null;
    final int[] readColIds = args.length > 2 && args[2] != null && !args[2].toString().isEmpty() ?
      Arrays.stream(args[2].toString().split(",")).mapToInt(Integer::valueOf).toArray() : null;

    RegionFunctionContext rfc = (RegionFunctionContext) functionContext;
    Region<Object, Object> localData = PartitionRegionHelper.getLocalData(rfc.getDataSet());

    MTableDescriptor td = MCacheFactory.getAnyInstance().getTable(tableName).getTableDescriptor();

    if (rfc.getFilter() == null || rfc.getFilter().isEmpty()) {
      executeOnData(td, localData.values(), ph, readColIds, functionContext.getResultSender());
    } else {
      executeOnData(td, localData.getAll(rfc.getFilter()).values(), ph, readColIds, functionContext.getResultSender());
    }
    functionContext.getResultSender().lastResult(null);
  }

  /**
   * Execute and send the matched results..
   * <p>
   *
   * @param tableDesc    the table descriptor
   * @param values       the list of values to be operated on
   * @param phs          the predicate holders
   * @param colIds       the column ids to fetch
   * @param resultSender result send object that sends matched results
   */
  @SuppressWarnings("unchecked")
  private void executeOnData(final MTableDescriptor tableDesc, final Collection<Object> values,
                             final MPredicateHolder[] phs, int[] colIds, final ResultSender<Object> resultSender) {
    List<Cell> cells;
    if (phs == null || phs.length == 0) {
      for (final Object e : values) {
        cells = MResultParser.getCells(tableDesc, (byte[]) e, colIds);
        resultSender.sendResult(cells);
      }
    } else {
      Predicate predicate;
      for (final Object e : values) {
        cells = MResultParser.getCells(tableDesc, (byte[]) e, colIds);
        boolean ret = true;
        for (final MPredicateHolder ph : phs) {
          predicate = ph.getPredicate();
          if (!predicate.test(cells.get(ph.columnIdx).getColumnValue())) {
            ret = false;
            break;
          }
        }
        if (ret) {
          resultSender.sendResult(cells);
        }
      }
    }
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
