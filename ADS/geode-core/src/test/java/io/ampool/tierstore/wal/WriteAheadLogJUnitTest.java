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
package io.ampool.tierstore.wal;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.FTableTest;

@Category(FTableTest.class)
public class WriteAheadLogJUnitTest {
  WriteAheadLog writeAheadLog = null;

  private void cleanDir(String dirName) {
    File dir = new File(dirName);
    String[] files = dir.list();
    if (files != null) {
      for (String f : files) {
        File file = new File(dirName + "/" + f);
        file.delete();
      }
    }
    dir.delete();
  }

  @Before
  public void setup() throws IOException {
    cleanDir("/tmp/WALDIR");
    WriteAheadLog.getInstance().deinit();
    Exception e = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"));
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
  }

  @After
  public void cleanUp() {
    Exception e = null;
    try {
      writeAheadLog.deinit();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    // cleanDir("/tmp/WALDIR");
  }

  @Test
  public void testWriteAndRead() {
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1);
    bv.checkAndAddRecord(data);
    Exception e = null;
    try {
      writeAheadLog.append("testTable", 1, new BlockKey(1l), bv);
    } catch (Exception e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);

    WALReader reader = null;
    try {
      reader =
          writeAheadLog.getReader("testTable_" + 1 + "_0" + WriteAheadLog.WAL_INPROGRESS_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);

    WALRecord record = null;
    try {
      record = reader.readNext();
    } catch (IOException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    assertNotNull(record);
    WALRecordHeader header = record.getHeader();

    assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
        record.getWALBytes().length);
    assertEquals(1, header.getBlockKey().getStartTimeStamp());
    assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));

    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
  }


  @Test
  public void testWriteAndReadMultiRecord() {
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;

    for (int i = 0; i < 100; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (Exception e1) {
        e = e1;
      }
      assertNull(e);
    }


    WALReader reader = null;
    try {
      reader =
          writeAheadLog.getReader("testTable_" + 1 + "_0" + WriteAheadLog.WAL_INPROGRESS_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);
    int cnt = 100;

    for (int i = 0; i < cnt; i++) {
      WALRecord record = null;
      try {
        record = reader.readNext();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
      assertNotNull(record);
      WALRecordHeader header = record.getHeader();

      assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
          record.getWALBytes().length);
      assertEquals(i, header.getBlockKey().getStartTimeStamp());

      assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));
    }
    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
  }

  @Test
  public void testWriteAndReadMultiRecordMultiFile() {
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;

    for (int i = 0; i < 200; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (Exception e1) {
        e = e1;
      }
      assertNull(e);
    }

    WALReader reader = null;
    try {
      reader = writeAheadLog.getReader("testTable_" + 1 + "_0" + WriteAheadLog.WAL_DONE_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);
    int cnt = 100;
    int seq = 1;

    for (int i = 0; i < cnt; i++) {
      WALRecord record = null;
      try {
        record = reader.readNext();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
      assertNotNull(record);
      WALRecordHeader header = record.getHeader();

      assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
          record.getWALBytes().length);
      assertEquals(i, header.getBlockKey().getStartTimeStamp());


      seq++;

      assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));
    }
    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }

    try {
      reader =
          writeAheadLog.getReader("testTable_" + 1 + "_1" + WriteAheadLog.WAL_INPROGRESS_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);
    cnt = 100;
    seq = 102;

    for (int i = 0; i < cnt; i++) {
      WALRecord record = null;
      try {
        record = reader.readNext();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
      assertNotNull(record);
      WALRecordHeader header = record.getHeader();

      assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
          record.getWALBytes().length);
      assertEquals(100 + i, header.getBlockKey().getStartTimeStamp());
      seq++;

      assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));

    }
    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
  }

  @Test
  public void testWriteAndReadMultiRecordMultiFileReinit() {
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;


    for (int i = 0; i < 200; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (Exception e1) {
        e = e1;
      }
      assertNull(e);
    }

    WALReader reader = null;
    try {
      reader = writeAheadLog.getReader("testTable_" + 1 + "_0" + WriteAheadLog.WAL_DONE_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);
    int cnt = 100;
    int seq = 1;

    for (int i = 0; i < cnt; i++) {
      WALRecord record = null;
      try {
        record = reader.readNext();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
      assertNotNull(record);
      WALRecordHeader header = record.getHeader();

      assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
          record.getWALBytes().length);
      assertEquals(i, header.getBlockKey().getStartTimeStamp());


      seq++;

      assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));

    }
    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }

    try {
      reader =
          writeAheadLog.getReader("testTable_" + 1 + "_1" + WriteAheadLog.WAL_INPROGRESS_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);
    cnt = 100;
    seq = 102;

    for (int i = 0; i < cnt; i++) {
      WALRecord record = null;
      try {
        record = reader.readNext();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
      assertNotNull(record);
      WALRecordHeader header = record.getHeader();

      assertEquals(Bytes.SIZEOF_INT + WALRecordHeader.SIZE + bv.getBytes().length,
          record.getWALBytes().length);
      assertEquals(100 + i, header.getBlockKey().getStartTimeStamp());
      seq++;

      assertEquals(0, Bytes.compareTo(record.getRecords()[0], data));

      try {
        writeAheadLog.deinit();
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);

      try {
        writeAheadLog.init(Paths.get("/tmp/WALDIR"));
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);

    }
    try {
      reader.close();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);


    for (int i = 0; i < 200; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (Exception e1) {
        e = e1;
      }
      assertNull(e);
    }

    for (int i = 0; i < 200; i++) {
      try {
        writeAheadLog.append("testTable", 2, new BlockKey(i), bv);
      } catch (Exception e1) {
        e = e1;
      }
      assertNull(e);
    }

    reader = null;
    try {
      reader = writeAheadLog.getReader("testTable_" + 1 + "_2" + WriteAheadLog.WAL_DONE_SUFFIX);
    } catch (FileNotFoundException e1) {
      e = e1;
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(reader);

  }

  @Test
  public void testFileRecordLimit() {
    cleanUp();
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;
    WriteAheadLog writeAheadLog = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"), 1001, 10);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }
    String[] inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(1, inprogressFiles.length);
    String[] completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(0, completedFiles.length);
    try {
      writeAheadLog.append("testTable", 1, new BlockKey(1002), bv);
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    assertNull(e);
    inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(1, inprogressFiles.length);
    completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(1, completedFiles.length);
  }


  @Test
  public void testFileDelete() {
    cleanUp();
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;
    WriteAheadLog writeAheadLog = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"), 10, 10);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }
    String[] inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(1, inprogressFiles.length);
    String[] completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(100, completedFiles.length);


    for (String file : inprogressFiles) {
      writeAheadLog.deleteWALFile(file);
    }

    for (String file : completedFiles) {
      writeAheadLog.deleteWALFile(file);
    }

    inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(0, inprogressFiles.length);
    completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(0, completedFiles.length);
  }


  @Test
  public void testNameComponents() {
    Object[][] names =
        {{"table_with_under_score_in_name_1_2", "table_with_under_score_in_name", 1, 2l},
            {"invalid", null, -1, -1l}, {"one_1", null, -1, -1l}, {"two_2_1", "two", 2, 1l},
            {"", null, -1, -1l}};
    for (int i = 0; i < names.length; i++) {
      String tableName = WriteAheadLog.getInstance().getTableName((String) names[i][0]);
      Integer partitionId = WriteAheadLog.getInstance().getPartitionId((String) names[i][0]);
      Long seqNo = WriteAheadLog.getInstance().getSeqNumber((String) names[i][0]);
      if (names[i][1] != null) {
        assertEquals(0, tableName.compareTo((String) names[i][1]));
      } else {
        assertNull(tableName);
      }
      if (names[i][2] != null) {
        assertEquals((int) names[i][2], (int) partitionId);
      } else {
        assertNull(partitionId);
      }
      if (names[i][3] != null) {
        assertEquals((long) names[i][3], (long) seqNo);
      } else {
        assertNull(seqNo);
      }
    }
  }

  @Test
  public void testTableDelete() {
    cleanUp();
    String tableName = "testTable_1";
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;
    WriteAheadLog writeAheadLog = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"), 10, 10);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append(tableName, 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }

    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append(tableName + "_2", 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }

    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append(tableName.split("_")[0], 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }

    String[] inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(3, inprogressFiles.length);
    String[] completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(300, completedFiles.length);

    writeAheadLog.deleteTable(tableName);

    assertEquals(0, writeAheadLog.getAllFilesForTable(tableName).length);

    completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(200, completedFiles.length);
    for (String file : completedFiles) {
      assertTrue(file.matches(tableName + "_2" + "_\\d+" + "_\\d+" + WriteAheadLog.WAL_DONE_SUFFIX)
          || file.matches(
              tableName.split("_")[0] + "_\\d+" + "_\\d+" + WriteAheadLog.WAL_DONE_SUFFIX));
    }
    inprogressFiles = writeAheadLog.getInprogressFiles();
    for (String file : inprogressFiles) {
      assertTrue(file
          .matches(tableName + "_2" + "_\\d+" + "_\\d+" + WriteAheadLog.WAL_INPROGRESS_SUFFIX)
          || file.matches(
              tableName.split("_")[0] + "_\\d+" + "_\\d+" + WriteAheadLog.WAL_INPROGRESS_SUFFIX));
    }

    assertEquals(2, inprogressFiles.length);
  }

  @Test
  public void testFileExpiry() {
    cleanUp();
    byte[] data = {1, 2, 3};
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    Exception e = null;
    WriteAheadLog writeAheadLog = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"), 1000, 1);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
    for (int i = 0; i < 1001; i++) {
      try {
        writeAheadLog.append("testTable", 1, new BlockKey(i), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }

    String[] inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(1, inprogressFiles.length);
    String[] completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(1, completedFiles.length);

    /* Sleep for 61 seconds and get expired files */
    try {
      Thread.sleep(61000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    String[] expired_files = writeAheadLog.getExpiredFiles();
    for (String file : expired_files) {
      System.out.println("File is : " + file);
    }
  }


  @Test
  public void testBucketDelete() {
    String tableName = "testTable_testBucketDelete";
    Exception e = null;
    final byte[] data = new byte[3];
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    for (int i = 0; i < 101; i++) {
      try {
        WriteAheadLog.getInstance().append(tableName, 0, new BlockKey(1), bv);
        WriteAheadLog.getInstance().append(tableName, 1, new BlockKey(1), bv);
        WriteAheadLog.getInstance().append(tableName, 2, new BlockKey(1), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }
    String[] files1 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 0);
    assertEquals(2, files1.length);

    String[] files2 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 1);
    assertEquals(2, files2.length);

    String[] files3 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 2);
    assertEquals(2, files3.length);

    WriteAheadLog.getInstance().deleteBucket(tableName, 1);

    files1 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 0);
    assertEquals(2, files1.length);

    files2 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 1);
    assertEquals(0, files2.length);

    files3 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 2);
    assertEquals(2, files3.length);

    WriteAheadLog.getInstance().deleteBucket(tableName, 0);

    files1 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 0);
    assertEquals(0, files1.length);

    files2 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 1);
    assertEquals(0, files2.length);

    files3 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 2);
    assertEquals(2, files3.length);
  }

  @Test
  public void testMarkFileDone() {
    String tableName = "table_testMarkFileDone";
    Exception e = null;
    final byte[] data = new byte[3];
    BlockValue bv = new BlockValue(1000);
    bv.checkAndAddRecord(data);
    for (int i = 0; i < 101; i++) {
      try {
        WriteAheadLog.getInstance().append(tableName, 0, new BlockKey(1), bv);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }
    String[] files1 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 0);
    assertEquals(2, files1.length);

    assertTrue(files1[0].compareTo(tableName + "_0_0.done") == 0
        || files1[1].compareTo(tableName + "_0_0.done") == 0);
    assertTrue(files1[0].compareTo(tableName + "_0_1.inprogress") == 0
        || files1[1].compareTo(tableName + "_0_1.inprogress") == 0);

    String result = null;
    try {
      result =
          WriteAheadLog.getInstance().markFileDone("/tmp/WALDIR/" + tableName + "_0_1.inprogress");
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(result);
    assertTrue(result.compareTo("/tmp/WALDIR/" + tableName + "_0_1.done") == 0);

    try {
      result = WriteAheadLog.getInstance().markFileDone("/tmp/WALDIR/" + tableName + "_0_0.done");
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    assertNotNull(result);
    assertTrue(result.compareTo("/tmp/WALDIR/" + tableName + "_0_0.done") == 0);


    files1 = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, 0);
    assertEquals(2, files1.length);

    assertTrue(files1[0].compareTo(tableName + "_0_0.done") == 0
        || files1[1].compareTo(tableName + "_0_0.done") == 0);
    assertTrue(files1[0].compareTo(tableName + "_0_1.done") == 0
        || files1[1].compareTo(tableName + "_0_1.done") == 0);

  }
}
