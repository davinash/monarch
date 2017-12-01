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
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.readers.orc.TierStoreORCReader;
import io.ampool.tierstore.stores.LocalTierStore;
import io.ampool.tierstore.writers.orc.TierStoreORCWriter;
import org.apache.geode.test.junit.categories.StoreTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(StoreTest.class)
public class LocalTierStoreTestWithComplexTypes {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @Before
  public void BeforeTest() {
    LocalTierStore.test_Hook_disable_stats = false;
  }

  @Test
  public void testORCWriting() {
    File orcBaseDir = null;
    String tableName = "testORCWriting";
    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }
    LocalTierStore lts = new LocalTierStore("local-disk-tier-store");
    TierStoreWriter tsw = new TierStoreORCWriter();
    tsw.init(StoreTestUtils.getWriterProps(FileFormats.ORC));
    TierStoreReader tsr = new TierStoreORCReader();
    // tsr.init(new Properties());

    Properties storeProps = new Properties();
    storeProps.put("base.dir.path", orcBaseDir.getAbsolutePath());
    int partitionId = 1;
    int numberOfPartitionFolderExpected = 1;
    final ConverterDescriptor converterDescriptorComplexTypes =
        StoreTestUtils.getConverterDescriptorComplexTypes(tableName, FileFormats.ORC);
    try {
      lts.init(storeProps, tsw, tsr);
      final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
      writer.setConverterDescriptor(converterDescriptorComplexTypes);
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        int noOfrecords =
            writer.write(StoreTestUtils.getCommonConfigProps(tableName, partitionId, orcBaseDir),
                getStoreRecords(5000));
        assertEquals(5000, noOfrecords);
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
    String tableName = "testORCWritingReadingWithList";
    int partitionId = 1;
    int batchSize = 100;
    int numberOfPartitionFolderExpected = 1;

    try {
      orcBaseDir = folder.newFolder("ORC");
    } catch (IOException e) {
      e.printStackTrace();
    }

    LocalTierStore lts = createStore(orcBaseDir.getAbsolutePath());
    final ConverterDescriptor converterDescriptorComplexTypes =
        StoreTestUtils.getConverterDescriptorComplexTypes(tableName, FileFormats.ORC);
    final TierStoreWriter writer = lts.getWriter(tableName, partitionId);
    writer.setConverterDescriptor(converterDescriptorComplexTypes);

    try {
      for (int i = 0; i < numberOfPartitionFolderExpected; i++) {
        List<StoreRecord> storeRecords = Arrays.asList(getStoreRecords(batchSize));
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
    reader.setConverterDescriptor(converterDescriptorComplexTypes);

    int readCount = 0;
    final Iterator<StoreRecord> itr = reader.iterator();
    while (itr.hasNext()) {
      final StoreRecord next = itr.next();
      // TODO validate record
      readCount++;
    }
    assertEquals(batchSize * numberOfPartitionFolderExpected, readCount);
  }

  private LocalTierStore createStore(final String orcBaseDirPath) {
    LocalTierStore lts = null;
    try {
      lts = new LocalTierStore("local-disk-tier-store");
      TierStoreWriter tsw = new TierStoreORCWriter();
      tsw.init(StoreTestUtils.getWriterProps(FileFormats.ORC));
      TierStoreReader tsr = new TierStoreORCReader();

      Properties storeProps = new Properties();
      storeProps.put("base.dir.path", orcBaseDirPath);
      lts.init(storeProps, tsw, tsr);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lts;
  }

  public StoreRecord[] getStoreRecords(final int noOfRecords) {
    StoreRecord[] storeRecords = new StoreRecord[noOfRecords];
    for (int i = 0; i < noOfRecords; i++) {
      storeRecords[i] = StoreTestUtils.getStoreRecordWithComplexTypes();
    }
    return storeRecords;
  }



}
