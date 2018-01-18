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
package io.ampool.monarch.table.region;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.coprocessor.MObserverContext;
import io.ampool.monarch.table.coprocessor.MTableObserver;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorManager;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorUtils;
import io.ampool.monarch.table.coprocessor.internal.MObserverContextImpl;
import io.ampool.monarch.table.exceptions.MCheckOperationFailException;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.internal.MOpInfo;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableStorageFormatter;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowTupleBytesUtils {

  private static final Logger logger = LogService.getLogger();

  /**
   * Reads the MetaData for the table specified by name.
   */


  public static boolean invokePreGet(String regionName, Get get) {
    List<MTableObserver> observersList = MCoprocessorManager.getObserversList(regionName);
    boolean bypass = false;
    for (MTableObserver observer : observersList) {
      MObserverContext mObserverContext = MCoprocessorUtils.getMObserverContext(regionName);

      observer.preGet(mObserverContext, get);

      /*
       * if(e.coplete && e.bypass){ return value; }
       *
       * MTableRow row = new MTableRow(<ROWID>); row.addColumnValue("COLUMNNAME" , COLUYMNVALUE)
       */
      bypass = mObserverContext.getBypass() || bypass;
    }
    return bypass;
  }

  public static void invokePostGet(String regionName, Get get) {
    {
      List<MTableObserver> observersList = MCoprocessorManager.getObserversList(regionName);
      for (MTableObserver observer : observersList) {
        MObserverContextImpl mObserverContext = MCoprocessorUtils.getMObserverContext(regionName);
        observer.postGet(mObserverContext, get);
      }
    }
  }

  private static boolean inColList(List<Integer> cols, Integer o, boolean isCheckOp) {
    /*
     * The over-loading of the columnPositions to hold the check column requires that we omit the
     * first column for a check and put/delete when checking the list as the first entry in this
     * case is the check column and not one of the put/delete operationn columns.
     */
    if (o == null || cols.size() == 0) {
      return false;
    }
    for (int i = 0; i < cols.size(); i++) {
      // if (i == 0 && isCheckOp) continue;
      if (o.equals(cols.get(i))) {
        return true;
      }
    }
    return false;
  }


  public static byte[] getTimeStampFromByteValue(byte[] value) {
    final int timeStampLength = Bytes.SIZEOF_LONG;
    byte[] valByteArray = (byte[]) value;
    byte[] timeStamp = new byte[timeStampLength];
    System.arraycopy(valByteArray, 0, timeStamp, 0, timeStampLength);
    return timeStamp;
  }

  /*
   * Helper function to remove the timestamp from the value Value is stored in Cache with TimeStamp,
   * But for calculation and further computation we do not need Timestamp. This method just removes
   * 8 bytes of the timestamp.
   */
  public static byte[] removeTimeStamp(byte[] value) {
    final int timeStampLength = Bytes.SIZEOF_LONG;
    byte[] valueWithoutTimeStamp = new byte[value.length - timeStampLength];
    System.arraycopy(value, timeStampLength, valueWithoutTimeStamp, 0,
            value.length - timeStampLength);
    return valueWithoutTimeStamp;
  }

  public static byte[] addTimeStamp(byte[] retVal, Long timestamp) {
    final int timeStampLength = Bytes.SIZEOF_LONG;
    byte[] newVal = new byte[retVal.length + timeStampLength];
    byte[] tsVal = Bytes.toBytes(timestamp);
    System.arraycopy(tsVal, 0, newVal, 0, tsVal.length);
    System.arraycopy(retVal, 0, newVal, tsVal.length, retVal.length);
    return newVal;
  }

  /**
   * Get the appropriate new value for replicated table (i.e. not user-table).
   *
   * @param event the current put event
   * @return null if the operation is successful; the appropriate exception otherwise
   */
  public static Object getNewValue_0(final EntryEventImpl event) {
    if (event.getKey() instanceof MTableKey && event.getValue() instanceof byte[]) {
      return event.getValue();
    } else if (event.getValue() instanceof VMCachedDeserializable) {
      MValue v;
      v = (MValue) ((VMCachedDeserializable) event.getValue()).getDeserializedForReading();
      return v.getValue();
    } else if (event.getValue() instanceof MValue) {
      MValue v = (MValue) event.getValue();
      return v.getValue();
    }
    return null;
  }

  /**
   * Get the appropriate new value for the table types like ORDERED and UNORDERED.
   * <p>
   * The old-value and current-value are merged correctly to reflect the partial put or delete (only
   * few columns) operations. The resultant value is set as the latest value and will be available
   * for further operations (get/scan).
   * <p>
   * Currently, both the cases, information passed in key and value, are handled but going ahead we
   * can keep the key minimal and pass the required information in value only.
   * <p>
   * Returns the following exceptions, if the operation is unsuccessful: -
   * {@link RowKeyDoesNotExistException} if the specified key does not exist -
   * {@link MCheckOperationFailException} if the specified check does not pass
   *
   * @param event the current put event
   * @param td the table descriptor
   * @return null if the operation is successful; the appropriate exception otherwise
   */
  public static Object getNewValue_1(final EntryEventImpl event, final MTableDescriptor td)
          throws MException {
    /** partial changes only for ordered tables **/
    Object key = event.getKey();
    Object newValue = event.getValue();
    Object oldValue = event.getOldValue();
    byte[] newValueBytes = null;
    MOpInfo opInfo = null;

    /** import data case.. nothing to be done **/
    // TODO re look for conditions
    // System.out.println("RowTupleBytesUtils.getNewValue_1 :: " + "value instance: "+ newValue+ "
    // opInfo "+opInfo);
    // if (opInfo == null || opInfo.getOp() == MOperation.NONE
    // /* for the case where data from v1.0 is imported.. still it does not handle the case where
    // * selected columns were put as last operation.
    // */
    // || (key instanceof MTableKey && ((MTableKey) key).getColumnPositions() == null)) {
    // System.out.println("RowTupleBytesUtils.getNewValue_1 :: " + "returning....");
    // return null;
    // }
    /** old value will still be byte-array since at the end update we will set that **/
    return updateValue(event, td);
  }

  /**
   * Merge and update the value correctly. The partial put/delete are taken care of by merging the
   * old and new values. In case the operation could not be successfully completed, the respective
   * exception is returned indicating to stop further processing.
   * <p>
   * Returns the following exceptions, if the operation is unsuccessful: -
   * {@link RowKeyDoesNotExistException} if the specified key does not exist -
   * {@link MCheckOperationFailException} if the specified check does not pass
   *
   * @param event the current put event
   * @param tableDescriptor the tabledescriptor
   * @return null if successful; the respective exception otherwise
   */
  private static Object updateValue(final EntryEventImpl event,
                                    final MTableDescriptor tableDescriptor) throws MException {

    StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(tableDescriptor);

    Object newValue = event.getValue();
    Object oldValueBytes = event.getOldValue();
    MOpInfo opInfo = event.getOpInfo();
    MValue mValue = null;
    List<MValue> mValues = null;
    if (newValue instanceof VMCachedDeserializable) {
      // /** extract the byte-array **/
      Object tempVal;
      tempVal = ((VMCachedDeserializable) newValue).getDeserializedForReading();
      if (tempVal instanceof MValue) {
        mValue = (MValue) tempVal;
      } else if (tempVal instanceof List) {
        // multiversion batch put case
        mValues = (List) tempVal;
      }
    } else if (newValue instanceof MValue) { // Tx Case
      mValue = (MValue) newValue;
    }
    if (opInfo == null) {
      return newValue;
    }

    final MOperation op = opInfo.getOp();
    final MOpInfo.MCondition condition = opInfo.getCondition();
    event.setOpInfo(opInfo);

    if (event.getCallbackArgument() instanceof Long) {
      // timestamp is set from here
      Long callbackArgument = (Long) event.getCallbackArgument();
      if (callbackArgument != 0)
        opInfo.setTimestamp(callbackArgument);
    }

    if (op == MOperation.CHECK_AND_DELETE || op == MOperation.CHECK_AND_PUT) {
      if (oldValueBytes == null) {
        if (condition == null || condition.getColumnValue() != null) {
          // if no condition is specified or value specified is not null then throw exception
          throw new RowKeyDoesNotExistException(RowKeyDoesNotExistException.MSG);
        } else {
          if (condition == null || condition.getColumnValue() == null) {
            // here we should condition
            // this is like hbase where absence is marked with null
            // TODO For ampool should we really do this??
            // EMPTY IF can be removed when TODO is confirmed
          }
        }
      } else if (condition == null) {
        throw new MCheckOperationFailException("No check-condition specified.");
      } else {
        if (op == MOperation.CHECK_AND_DELETE) {
          return storageFormatter.performCheckAndDeleteOperation(tableDescriptor, opInfo,
                  oldValueBytes);
        }
        if (op == MOperation.CHECK_AND_PUT) {
          return storageFormatter.performCheckAndPutOperation(tableDescriptor, mValue, opInfo,
                  oldValueBytes);
        }
      }
    }
    if (op == MOperation.DELETE) {
      return storageFormatter.performDeleteOperation(tableDescriptor, opInfo, oldValueBytes);
    }
    if (op == MOperation.PUT) {
      if (mValues != null) {
        // iterate through list and perform put operation
        Object changedValue = oldValueBytes;
        for (int i = 0; i < mValues.size(); i++) {
          changedValue = storageFormatter.performPutOperation(tableDescriptor, mValues.get(i),
                  opInfo, changedValue);
        }
        return changedValue;
      } else {
        return storageFormatter.performPutOperation(tableDescriptor, mValue, opInfo, oldValueBytes);
      }
    }
    throw new MException();
  }

  /**
   * Merge and update the value correctly. The partial put/delete are taken care of by merging the
   * old and new values. In case the operation could not be successfully completed, the respective
   * exception is returned indicating to stop further processing.
   * <p>
   * Returns the following exceptions, if the operation is unsuccessful: -
   * {@link RowKeyDoesNotExistException} if the specified key does not exist -
   * {@link MCheckOperationFailException} if the specified check does not pass
   *
   * @param event the current put event
   * @param tableName the table-name
   * @param td the table descriptor
   * @param oldValueBytes the old/last value
   * @param newValueBytes the latest value to be used for merge
   * @param opInfo the operation information
   * @return null if successful; the respective exception otherwise
   */

  /**
   * Additional destroy mechanism for Ampool tables.
   *
   * @param region the LocalRegion handle
   * @param event the EntryEventImpl
   */
  public static void destroy_0(final LocalRegion region, EntryEventImpl event) {
    MOpInfo opInfo = event.getOpInfo();
    MTableDescriptor td =
            MCacheFactory.getAnyInstance().getMTableDescriptor(region.getDisplayName());
    Object getVal = event.getOldValue();
    if (getVal == null) {
      try {
        getVal = region.getValueOnDisk(event.getKey());// this is evicted or it is non existent
        // key See GEN-1759
      } catch (EntryNotFoundException ex) {
        throw new RowKeyDoesNotExistException("Row Id does not exists");
      }

      if (getVal == null) {
        throw new RowKeyDoesNotExistException("Row Id does not exists");
      }
    }

    StorageFormatter storageFormatter = MTableUtils.getStorageFormatter(td);

    // GEN 380
    if (opInfo.getOp() == MOperation.CHECK_AND_DELETE) {
      MOpInfo.MCondition condition = opInfo.getCondition();
      if (condition == null) {
        throw new MCheckOperationFailException("No check-condition specified.");
      }
      boolean checkPassed = ((MTableStorageFormatter) storageFormatter).checkValue(td, getVal,
              (byte[]) condition.getColumnValue(), condition.getColumnId());
      if (!checkPassed) {
        throw new MCheckOperationFailException("delete check failed");
      }
    } else if (opInfo.getOp() == MOperation.DELETE && td.getMaxVersions() == 1) {
      // this should be the case with delete with timestamp so if TS is not then
      // throw exception and abort the delete

      if (storageFormatter instanceof MTableStorageFormatter) {
        if (opInfo.getTimestamp() != 0) {
          // In case of single version table we should only delete if timestamp matches.
          boolean matches = ((MTableStorageFormatter) storageFormatter)
                  .matchesTimestamp(CompareOp.EQUAL, (byte[]) getVal, opInfo.getTimestamp());

          if (!matches) {
            // we should not delete
            throw new MCheckOperationFailException("Record with given timestamp "
                    + opInfo.getTimestamp() + " not found. Delete operation failed");
          }
        }
      }
    }
  }

  public static Object adjustValueForAmpool(final EntryEventImpl event) throws MException {
    final String tableName = event.getRegion().getDisplayName();
    final TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);;
    if (td instanceof MTableDescriptor) {
      MTableDescriptor mTableDescriptor = (MTableDescriptor) td;
      if (!mTableDescriptor.getUserTable()) {
        return getNewValue_0(event);
      } else if (mTableDescriptor.getTableType() == MTableType.UNORDERED
              || mTableDescriptor.getTableType() == MTableType.ORDERED_VERSIONED) {
        return RowTupleBytesUtils.getNewValue_1(event, mTableDescriptor);
      }
    }
    return null;
  }

  public static Object prepareValueForAmpoolCache(final EntryEventImpl event) {
    Object val = null;
    if (event != null
            && AmpoolTableRegionAttributes.isAmpoolMTable(event.getRegion().getCustomAttributes())) {
      val = RowTupleBytesUtils.adjustValueForAmpool(event);
    }
    return val;
  }
}
