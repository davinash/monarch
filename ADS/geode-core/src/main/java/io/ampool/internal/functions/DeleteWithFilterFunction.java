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
package io.ampool.internal.functions;

import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.region.ScanUtils;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DeleteWithFilterFunction implements Function, InternalEntity {

  @Override
  public void execute(FunctionContext context) {
    try {
      // get the parameters map of row key and filters
      List arguments = (ArrayList) context.getArguments();
      final String tablename = (String) arguments.get(0);
      final Map<Delete, Filter> deleteFilterMap = (Map<Delete, Filter>) arguments.get(1);
      deleteWithFilter(tablename, deleteFilterMap);
      context.getResultSender().lastResult(true);
    } catch (Exception ex) {
      context.getResultSender().sendException(ex);
    }
  }

  /**
   * Deletes all keys and version which are matched by the filter
   * 
   * @param tablename name of the table
   * @param deleteFilterMap key and filter to be applied for each key
   */
  public void deleteWithFilter(final String tablename, final Map<Delete, Filter> deleteFilterMap) {
    PartitionedRegion pr = (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tablename);
    Iterator<Map.Entry<Delete, Filter>> iterator = deleteFilterMap.entrySet().iterator();
    MTableDescriptor tableDescriptor =
        MCacheFactory.getAnyInstance().getMTableDescriptor(tablename);
    while (iterator.hasNext()) {
      Map.Entry<Delete, Filter> deleteFilterEntry = iterator.next();
      if (deleteFilterEntry.getValue() != null) {
        MKeyBase mKeyBase = new MKeyBase(deleteFilterEntry.getKey().getRowKey());
        BucketRegion primaryBucketForKey = getPrimaryBucketForKey(pr, mKeyBase);
        if (primaryBucketForKey != null) {
          Object value = primaryBucketForKey.get(mKeyBase);
          Object valueAfterFilter = performDeleteOperationWithFilter(tableDescriptor, value,
              deleteFilterEntry.getValue());
          if (valueAfterFilter == null) {
            pr.destroy(mKeyBase);
          } else if (tableDescriptor.getMaxVersions() > 1) {
            pr.put(mKeyBase, valueAfterFilter, null);
          }
        } // no primary bucket for this key
      } // filter is null so no need to apply filter
    } // end while
  }

  /**
   * gets the primary bucket region associated with the key on this server
   * 
   * @param partitionedRegion partition region instance
   * @param key key
   * @return bucket region instance for the key
   */
  private BucketRegion getPrimaryBucketForKey(PartitionedRegion partitionedRegion, Object key) {
    int bucketId = PartitionedRegionHelper.getHashKey(partitionedRegion, null, key, null, null);
    BucketRegion localBucketById = partitionedRegion.getDataStore().getLocalBucketById(bucketId);
    if (localBucketById != null && localBucketById.getBucketAdvisor().isPrimary()) {
      return localBucketById;
    }
    return null;
  }

  /**
   * performs delete operation on the value with matching filter
   * 
   * @param tableDescriptor table descriptor instance
   * @param value value on which delete needs to be performed
   * @param filter filter to be applied before deleting
   * @return value after deleting all matched version/rows
   */
  private Object performDeleteOperationWithFilter(TableDescriptor tableDescriptor, Object value,
      Filter filter) {
    if (value == null) {
      throw new RowKeyDoesNotExistException("Row Id does not exists");
    }

    MTableDescriptor mTableDescriptor = (MTableDescriptor) tableDescriptor;
    final ThinRow row = ThinRow.create(mTableDescriptor, ThinRow.RowFormat.M_FULL_ROW);

    // if multi-versioned
    if (value instanceof MultiVersionValue) {
      MultiVersionValue multiVersionValue = (MultiVersionValue) value;
      byte[][] versions = multiVersionValue.getVersions();
      int valueSize = 0;
      final List<byte[]> newVersions = new LinkedList<>();
      for (int i = 0; i < versions.length; i++) {
        byte[] version = versions[i];
        if (version == null) {
          continue;
        }
        row.reset(null, version);
        if (!ScanUtils.executeSimpleFilter(row, filter, tableDescriptor)) {
          valueSize += version.length;
          newVersions.add(version);
        }
      }
      if (newVersions.isEmpty()) {
        return null;
      }
      multiVersionValue.setVersions(newVersions.toArray(new byte[1][]), valueSize);
      return multiVersionValue;
    } else {
      // single versions delete
      row.reset(null, (byte[]) value);
      if (!ScanUtils.executeSimpleFilter(row, filter, tableDescriptor)) {
        return value;
      }
    }
    return null;
  }
}
