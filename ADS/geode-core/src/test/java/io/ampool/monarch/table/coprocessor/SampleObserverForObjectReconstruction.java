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

package io.ampool.monarch.table.coprocessor;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.ByteArrayKey;

import java.util.List;
import java.util.Map;

/**
 * Sample Put observer
 *
 * @since 0.2.0.0
 */

public class SampleObserverForObjectReconstruction extends MBaseRegionObserver {

  private int preDelete;
  private int postDelete;

  private int preCheckAndDelete;
  private int postCheckAndDelete;

  private int prePut;
  private int postPut;

  private int preCheckAndPut;
  private int postCheckAndPut;

  public SampleObserverForObjectReconstruction() {
    this.preDelete = 0;
    this.postDelete = 0;
    this.preCheckAndDelete = 0;
    this.postCheckAndDelete = 0;

    this.prePut = 0;
    this.postPut = 0;
    this.preCheckAndPut = 0;
    this.postCheckAndPut = 0;
  }

  public int getTotalPreDeleteCount() {
    return preDelete;
  }

  public int getTotalPostDeleteCount() {
    return postDelete;
  }

  public int getTotalPreCheckAndDeleteCount() {
    return preCheckAndDelete;
  }

  public int getTotalPostCheckAndDeleteCount() {
    return postCheckAndDelete;
  }

  public int getTotalPrePutCount() {
    return prePut;
  }

  public int getTotalPostPutCount() {
    return postPut;
  }

  public int getTotalPreCheckAndPutCount() {
    return preCheckAndPut;
  }

  public int getTotalPostCheckAndPutCount() {
    return postCheckAndPut;
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "+++");
    String deleteObjRowKey = Bytes.toString(delete.getRowKey());
    String rowKey = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX + preDelete;
    assertEquals(rowKey, deleteObjRowKey);

    List<byte[]> cols = delete.getColumnNameList();
    if (cols.size() > 0) {
      for (int i = 0; i < cols.size() - 1; i++) {
        byte[] colName = cols.get(i);
        // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "Name: " +
        // Bytes.toString(colName));
        if (i == 0) {
          assertEquals("NAME", Bytes.toString(colName));
        } else if (i == 1) {
          assertEquals("ID", Bytes.toString(colName));
        } else {
          fail("No delete with more that two columns in test case");
        }
      }
    }
    preDelete++;
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "+++");
    String deleteObjRowKey = Bytes.toString(delete.getRowKey());
    String rowKey = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX + postDelete;
    assertEquals(rowKey, deleteObjRowKey);

    List<byte[]> cols = delete.getColumnNameList();
    if (cols.size() > 0) {
      for (int i = 0; i < cols.size() - 1; i++) {
        byte[] colName = cols.get(i);
        // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "Name: " +
        // Bytes.toString(colName));
        if (i == 0) {
          assertEquals("NAME", Bytes.toString(colName));
        } else if (i == 1) {
          assertEquals("ID", Bytes.toString(colName));
        } else {
          fail("No delete with more that two columns in test case");
        }
      }
    }
    postDelete++;
  }

  @Override
  public void preCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "+++");
    String deleteObjRowKey = Bytes.toString(delete.getRowKey());
    String rowKey1 =
        MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX + preCheckAndDelete;
    if (!isCheckSuccessful)
      rowKey1 = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
          + preCheckAndDelete % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    // System.out.println("SampleObserverForObjectReconstruction.preCheckAndDelete: " + "PreCount: "
    // + preCheckAndDelete + " rowKey"
    // + deleteObjRowKey);
    assertEquals(rowKey1, deleteObjRowKey);

    List<byte[]> cols = delete.getColumnNameList();
    if (cols.size() > 0) {
      for (int i = 0; i < cols.size() - 1; i++) {
        byte[] colName = cols.get(i);
        // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "Name: " +
        // Bytes.toString(colName));
        if (i == 0) {
          assertEquals("NAME", Bytes.toString(colName));
        } else if (i == 1) {
          assertEquals("ID", Bytes.toString(colName));
        } else {
          fail("No delete with more that two columns in test case");
        }
      }
    }
    preCheckAndDelete++;
  }

  @Override
  public void postCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey,
      byte[] columnName, byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    // System.out.println("SampleObserverForObjectReconstruction.postCheckAndDelete: " + "^^^^^^^");
    // System.out.println("SampleObserverForObjectReconstruction.postCheckAndDelete: " + "+++" +
    // postCheckAndDelete);
    String deleteObjRowKey = Bytes.toString(delete.getRowKey());
    String rowKey1 =
        MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX + postCheckAndDelete;
    if (!isCheckSuccessful)
      rowKey1 = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
          + postCheckAndDelete % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    assertEquals(rowKey1, deleteObjRowKey);

    List<byte[]> cols = delete.getColumnNameList();
    if (cols.size() > 0) {
      for (int i = 0; i < cols.size() - 1; i++) {
        byte[] colName = cols.get(i);
        // System.out.println("SampleObserverForObjectReconstruction.preDelete: " + "Name: " +
        // Bytes.toString(colName));
        if (i == 0) {
          assertEquals("NAME", Bytes.toString(colName));
        } else if (i == 1) {
          assertEquals("ID", Bytes.toString(colName));
        } else {
          fail("No delete with more that two columns in test case");
        }
      }
    }
    postCheckAndDelete++;
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    String putObjRowKey = Bytes.toString(put.getRowKey());
    String rowKey = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
        + prePut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    assertEquals(rowKey, putObjRowKey);

    Map<ByteArrayKey, Object> colValueMap = put.getColumnValueMap();
    colValueMap.forEach((k, v) -> {
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("NAME")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes(
            "Nilkanth" + prePut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries)));
      }
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("ID")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("AGE")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("SALARY")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes
            .toBytes(prePut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries + 10)));
      }
    });

    prePut++;
  }

  @Override
  public void preCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccess) {
    String putObjRowKey = Bytes.toString(put.getRowKey());
    String rowKey1 = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
        + preCheckAndPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    assertEquals(rowKey1, putObjRowKey);

    Map<ByteArrayKey, Object> colValueMap = put.getColumnValueMap();
    // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Map size:
    // "+colValueMap.size());
    colValueMap.forEach((k, v) -> {
      // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Col
      // Name:"+Bytes.toString(k.getByteArray()));
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("NAME")) {
        // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Name:
        // "+Bytes.toString(((byte[]) v)));
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes("Deepak"
            + preCheckAndPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries)));
      }
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("ID")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("AGE")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("SALARY")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes(
            preCheckAndPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries + 10)));
      }
    });
    preCheckAndPut++;
  }

  @Override
  public void postCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful) {
    String putObjRowKey = Bytes.toString(put.getRowKey());
    String rowKey1 = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
        + postCheckAndPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    assertEquals(rowKey1, putObjRowKey);

    Map<ByteArrayKey, Object> colValueMap = put.getColumnValueMap();
    // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Map size:
    // "+colValueMap.size());
    colValueMap.forEach((k, v) -> {
      // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Col
      // Name:"+Bytes.toString(k.getByteArray()));
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("NAME")) {
        // System.out.println("SampleObserverForObjectReconstruction.preCheckAndPut: " + "Name:
        // "+Bytes.toString(((byte[]) v)));
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes("Deepak"
            + postCheckAndPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries)));
      }
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("ID")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("AGE")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("SALARY")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes(postCheckAndPut + 10)));
      }
    });
    postCheckAndPut++;
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    String putObjRowKey = Bytes.toString(put.getRowKey());
    // System.out.println("SampleObserverForObjectReconstruction.postPut: " + "postPut: "+postPut);
    String rowKey = MTableCoprocessorMObjectReconstructionDUnitTest.ROW_KEY_PREFIX
        + postPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries;
    // System.out.println("SampleObserverForObjectReconstruction.postPut: " + "PutKey:
    // "+putObjRowKey);
    // System.out.println("SampleObserverForObjectReconstruction.postPut: " + "Cal key:"+rowKey);
    assertEquals(rowKey, putObjRowKey);

    Map<ByteArrayKey, Object> colValueMap = put.getColumnValueMap();
    colValueMap.forEach((k, v) -> {
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("NAME")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes.toBytes(
            "Nilkanth" + postPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries)));
      }
      if (Bytes.toString(k.getByteArray()).equalsIgnoreCase("ID")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("AGE")
          || Bytes.toString(k.getByteArray()).equalsIgnoreCase("SALARY")) {
        assertEquals(0, Bytes.compareTo(((byte[]) v), Bytes
            .toBytes(postPut % MTableCoprocessorMObjectReconstructionDUnitTest.numOfEntries + 10)));
      }
    });
    postPut++;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
