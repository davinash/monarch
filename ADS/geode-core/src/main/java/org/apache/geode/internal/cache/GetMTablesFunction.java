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
package org.apache.geode.internal.cache;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.TableType;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;

/**
 * Function that retrieves mtables hosted on every member
 *
 */
public class GetMTablesFunction extends FunctionAdapter implements InternalEntity {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return GetMTablesFunction.class.toString();
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      final HashMap<String, TableType> allTablesWithTypes = new HashMap<String, TableType>();
      Region metaRegion =
          MCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME);
      if (metaRegion == null || metaRegion.size() == 0) {
        functionContext.getResultSender().lastResult(allTablesWithTypes);
      }
      // final Set allTables = metaRegion.entrySet();
      final Set<Entry<String, TableDescriptor>> allTables = metaRegion.entrySet();

      allTables.forEach(entry -> {
        final String tableName = entry.getKey();
        TableDescriptor tableDescriptor = entry.getValue();
        if (tableDescriptor instanceof FTableDescriptor) {
          allTablesWithTypes.put(tableName, TableType.IMMUTABLE);
        } else {
          if (((MTableDescriptor) tableDescriptor).getTableType() == MTableType.UNORDERED) {
            allTablesWithTypes.put(tableName, TableType.UNORDERED);
          } else {
            allTablesWithTypes.put(tableName, TableType.ORDERED_VERSIONED);
          }
        }
      });
      functionContext.getResultSender().lastResult(allTablesWithTypes);
    } catch (CacheClosedException e) {
      functionContext.getResultSender().sendException(e);
    } catch (Exception e) {
      functionContext.getResultSender().sendException(e);
    }
  }
}
