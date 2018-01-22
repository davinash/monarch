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

package io.ampool.monarch.table.facttable.junit;

import static io.ampool.monarch.table.ftable.FTableDescriptor.BlockFormat.AMP_BYTES;
import static io.ampool.monarch.table.ftable.FTableDescriptor.BlockFormat.ORC_BYTES;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableDescriptorTest {

  private static final String MSG = "Incorrect default value for: ";

  /**
   * Test defaults for the FTableDescriptor.
   */
  @Test
  public void testDefaults() {
    final FTableDescriptor td = new FTableDescriptor();

    assertEquals(MSG + "block-size", 1000, td.getBlockSize());
    assertEquals(MSG + "block-format", FTableDescriptor.BlockFormat.AMP_BYTES, td.getBlockFormat());
    assertEquals(MSG + "column-statistics-enabled", true, td.isColumnStatisticsEnabled());
    assertEquals(MSG + "partitioning-column", null, td.getPartitioningColumn());
    assertEquals(MSG + "disk-persistence", true, td.isDiskPersistenceEnabled());
    assertEquals(MSG + "eviction-policy", MEvictionPolicy.OVERFLOW_TO_TIER, td.getEvictionPolicy());
    assertEquals(MSG + "disk-write-policy", MDiskWritePolicy.ASYNCHRONOUS, td.getDiskWritePolicy());
    assertEquals(MSG + "redundancy", 0, td.getRedundantCopies());
    assertEquals(MSG + "tier-stores", Collections.emptyMap(), td.getTierStores());
    assertEquals(MSG + "disk-store", MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME,
            td.getRecoveryDiskStore());
  }

  @Test
  public void setEvictionPolicy() throws Exception {
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.setEvictionPolicy(MEvictionPolicy.LOCAL_DESTROY);
    assertEquals(fTableDescriptor.getEvictionPolicy(), MEvictionPolicy.LOCAL_DESTROY);

    fTableDescriptor.setEvictionPolicy(MEvictionPolicy.NO_ACTION);
    assertEquals(fTableDescriptor.getEvictionPolicy(), MEvictionPolicy.NO_ACTION);

    fTableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
    assertEquals(fTableDescriptor.getEvictionPolicy(), MEvictionPolicy.OVERFLOW_TO_TIER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setEvictionPolicyExpectingException() throws Exception {
    FTableDescriptor fTableDescriptor = new FTableDescriptor();

    fTableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_DISK);
    assertEquals(fTableDescriptor.getEvictionPolicy(), MEvictionPolicy.OVERFLOW_TO_DISK);
  }

  @Test
  public void addTierStores() throws Exception {
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    assertEquals(0, fTableDescriptor.getTierStores().size());

    LinkedHashMap<String, TierStoreConfiguration> tiers = new LinkedHashMap<>();
    tiers.put("store1", new TierStoreConfiguration());
    fTableDescriptor.addTierStores(tiers);
    assertEquals(1, fTableDescriptor.getTierStores().size());

    tiers = new LinkedHashMap<>();
    tiers.put("store1", new TierStoreConfiguration());
    tiers.put("store2", new TierStoreConfiguration());
    tiers.put("store3", new TierStoreConfiguration());

    fTableDescriptor.addTierStores(tiers);
    assertEquals(tiers.size(), fTableDescriptor.getTierStores().size());
  }

  @Test
  public void addTierStores2() throws Exception {
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    assertEquals(0, fTableDescriptor.getTierStores().size());

    LinkedHashMap<String, TierStoreConfiguration> tiers = new LinkedHashMap<>();
    tiers.put("store1", new TierStoreConfiguration());
    fTableDescriptor.addTierStores(tiers);
    assertEquals(1, fTableDescriptor.getTierStores().size());

    tiers = new LinkedHashMap<>();

    TierStoreConfiguration tierStoreConfiguration1 = new TierStoreConfiguration();
    Properties properties1 = new Properties();
    properties1.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, "10");
    properties1.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL, "5");
    tierStoreConfiguration1.setTierProperties(properties1);
    tiers.put("store1", tierStoreConfiguration1);

    TierStoreConfiguration tierStoreConfiguration2 = new TierStoreConfiguration();
    Properties properties2 = new Properties();
    properties2.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, "10");
    properties2.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL, "5");
    tierStoreConfiguration2.setTierProperties(properties2);
    tiers.put("store2", tierStoreConfiguration2);

    TierStoreConfiguration tierStoreConfiguration3 = new TierStoreConfiguration();
    Properties properties3 = new Properties();
    properties3.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, "10");
    properties3.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL, "5");
    tierStoreConfiguration3.setTierProperties(properties3);
    tiers.put("store3", tierStoreConfiguration3);

    fTableDescriptor.addTierStores(tiers);
    assertEquals(tiers.size(), fTableDescriptor.getTierStores().size());

    LinkedHashMap<String, TierStoreConfiguration> tierStores = fTableDescriptor.getTierStores();
    assertEquals(tiers, tierStores);

    for (Map.Entry<String, TierStoreConfiguration> entry : tierStores.entrySet()) {
      assertEquals(2, entry.getValue().getTierProperties().size());
      assertEquals(TimeUnit.HOURS.toMillis(5),
              entry.getValue().getTierProperties().get(TierStoreConfiguration.TIER_PARTITION_INTERVAL));
      assertEquals(TimeUnit.HOURS.toMillis(10),
              entry.getValue().getTierProperties().get(TierStoreConfiguration.TIER_TIME_TO_EXPIRE));
    }
  }

  @Test
  public void testBlockAttributes() throws IOException, ClassNotFoundException {
    final FTableDescriptor td = new FTableDescriptor();
    assertEquals("Incorrect default block-size.", 1000, td.getBlockSize());
    assertEquals("Incorrect default block-format.", AMP_BYTES, td.getBlockFormat());
    assertEquals("Incorrect default block properties.", null, td.getBlockProperties());
    td.setBlockSize(10_000);
    td.setBlockFormat(ORC_BYTES);
    assertEquals("Incorrect block-size.", 10_000, td.getBlockSize());
    assertEquals("Incorrect block-format.", ORC_BYTES, td.getBlockFormat());

    Properties props = new Properties();
    props.setProperty("orc.block.size", "12345");
    props.setProperty("orc.row.index.stride", "1000");
    td.setBlockProperties(props);
    assertEquals("Incorrect block properties.", props, td.getBlockProperties());

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(os);
    td.toData(out);
    out.close();

    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

    final FTableDescriptor newTd = new FTableDescriptor();
    newTd.fromData(in);
    in.close();

    assertEquals("Incorrect block-size.", 10_000, newTd.getBlockSize());
    assertEquals("Incorrect block-format.", ORC_BYTES, newTd.getBlockFormat());
    assertEquals("Incorrect block properties.", props, newTd.getBlockProperties());
    String p = "orc.block.size";
    assertEquals("Incorrect block property: " + p, "12345",
            newTd.getBlockProperties().getProperty(p));
    p = "orc.row.index.stride";
    assertEquals("Incorrect block property: " + p, "1000",
            newTd.getBlockProperties().getProperty(p));
  }
}
