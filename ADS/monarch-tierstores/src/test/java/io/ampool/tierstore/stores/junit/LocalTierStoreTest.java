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

package io.ampool.tierstore.stores.junit;

import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.readers.orc.TierStoreORCReader;
import io.ampool.tierstore.readers.parquet.TierStoreParquetReader;
import io.ampool.tierstore.stores.LocalTierStore;
import io.ampool.tierstore.writers.orc.TierStoreORCWriter;
import io.ampool.tierstore.writers.parquet.TierStoreParquetWriter;
import org.apache.geode.test.junit.categories.StoreTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(StoreTest.class)
public class LocalTierStoreTest {
  @Rule
  public TestName name = new TestName();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void BeforeTest() {
    LocalTierStore.test_Hook_disable_stats = false;
  }

  @Test
  public void testORCWriting() {
    File orcBaseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), FileFormats.ORC);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.ORC));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        int noOfrecords =
            writer.write(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir),
                StoreTestUtils.getDummyStoreRecords(batchSize));
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);
  }

  @Test
  public void testParquetWriting() {
    File baseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      baseDir = folder.newFolder("Parquet");
    } catch (IOException e) {
      e.printStackTrace();
    }

    FileFormats parquet = FileFormats.PARQUET;
    LocalTierStore lts = createStore(baseDir.getAbsolutePath(), parquet);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(StoreTestUtils.getConverterDescriptor(tableName, true, parquet));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        int noOfrecords =
            writer.write(StoreTestUtils.getCommonConfigProps(tableName, partitionId, baseDir),
                StoreTestUtils.getDummyStoreRecords(batchSize));
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(baseDir.exists());
    assertEquals(1, baseDir.listFiles().length);
    final File tableFolder = new File(baseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);
  }

  @Test
  public void testORCWritingWithList() {
    File orcBaseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), FileFormats.ORC);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.ORC));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        List<StoreRecord> storeRecords =
            Arrays.asList(StoreTestUtils.getDummyStoreRecords(batchSize));
        int noOfrecords = writer.write(
            StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir), storeRecords);
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);
  }

  @Test
  public void testParquetWritingWithList() {
    File orcBaseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("Parquet");
    } catch (IOException e) {
      e.printStackTrace();
    }

    FileFormats parquet = FileFormats.PARQUET;
    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), parquet);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(StoreTestUtils.getConverterDescriptor(tableName, true, parquet));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        List<StoreRecord> storeRecords =
            Arrays.asList(StoreTestUtils.getDummyStoreRecords(batchSize));
        int noOfrecords = writer.write(
            StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir), storeRecords);
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);
  }

  @Test
  public void testORCWritingReadingWithList() {
    File orcBaseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), FileFormats.ORC);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.ORC));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        List<StoreRecord> storeRecords =
            Arrays.asList(StoreTestUtils.getDummyStoreRecords(batchSize));
        int noOfrecords = writer.write(
            StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir), storeRecords);
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);

    TierStoreReader reader = lts.getReader(tableName, partitionId);
    reader.init(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir));
    reader.setFilter(new StoreScan());
    reader.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.ORC));

    int readCount = 0;
    final Iterator<StoreRecord> itr = reader.iterator();
    while (itr.hasNext()) {
      final StoreRecord next = itr.next();
      // TODO validate record
      readCount++;
    }
    assertEquals(batchSize * numberOfPartitionFolderExpected, readCount);
  }

  @Test
  public void testParquetWritingReadingWithList() {
    File orcBaseDir = null;
    String tableName = name.getMethodName();
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("Parquet");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), FileFormats.PARQUET);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.PARQUET));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        List<StoreRecord> storeRecords =
            Arrays.asList(StoreTestUtils.getDummyStoreRecords(batchSize));
        int noOfrecords = writer.write(
            StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir), storeRecords);
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(1, tableFolder.listFiles().length);
    final File partitionFolder = new File(tableFolder, String.valueOf(partitionId));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);

    TierStoreReader reader = lts.getReader(tableName, partitionId);
    reader.init(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir));
    reader.setFilter(new StoreScan());
    reader.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.PARQUET));

    int readCount = 0;
    final Iterator<StoreRecord> itr = reader.iterator();
    while (itr.hasNext()) {
      final StoreRecord next = itr.next();
      // TODO validate record
      readCount++;
    }
    assertEquals(batchSize * numberOfPartitionFolderExpected, readCount);
  }


  @Test
  public void testDeleteStore() {
    File orcBaseDir = null;
    String tableName = "testDeleteStore";
    int partitionId = 1;
    int batchSize = 5000;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath(), FileFormats.ORC);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(
        StoreTestUtils.getConverterDescriptor(tableName, true, FileFormats.ORC));

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        int noOfrecords =
            writer.write(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir),
                StoreTestUtils.getDummyStoreRecords(batchSize));
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    partitionId = 2;
    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        int noOfrecords =
            writer.write(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir),
                StoreTestUtils.getDummyStoreRecords(batchSize));
        assertEquals(batchSize, noOfrecords);

        // sleep so that we can get records partitioned
        try {
          Thread.sleep(1000l);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertTrue(orcBaseDir.exists());
    assertEquals(1, orcBaseDir.listFiles().length);
    final File tableFolder = new File(orcBaseDir, tableName);
    assertEquals(2, tableFolder.listFiles().length);
    File partitionFolder = new File(tableFolder, String.valueOf(1));
    assertEquals(numberOfPartitionFolderExpected, partitionFolder.listFiles().length);
    lts.deletePartition(tableName, 1);
    assertFalse(partitionFolder.exists());

    // invalid partition
    partitionFolder = new File(tableFolder, String.valueOf(1));
    assertFalse(partitionFolder.exists());

    partitionFolder = new File(tableFolder, String.valueOf(3));
    assertFalse(partitionFolder.exists());

    lts.deleteTable(tableName);
    assertFalse(tableFolder.exists());

    // invalid table
    lts.deleteTable("invalid");

  }


  private LocalTierStore createStore(final String baseDirPath, FileFormats fileFormat) {
    LocalTierStore lts = null;
    try {
      lts = new LocalTierStore("local-disk-tier-store-" + fileFormat.name());

      TierStoreWriter tsw = new TierStoreORCWriter();
      if (fileFormat == FileFormats.PARQUET) {
        tsw = new TierStoreParquetWriter();
      }
      tsw.init(StoreTestUtils.getWriterProps(fileFormat));

      TierStoreReader tsr = new TierStoreORCReader();
      if (fileFormat == FileFormats.PARQUET) {
        tsr = new TierStoreParquetReader();
      }
      Properties storeProps = new Properties();
      storeProps.put("base.dir.path", baseDirPath);
      // for test purpose only
      storeProps.put("disable.evictor.thread", "false");
      lts.init(storeProps, tsw, tsr);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lts;
  }
}
