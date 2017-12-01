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
package io.ampool.monarch.hive;

import io.ampool.monarch.RMIException;
import io.ampool.monarch.TestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Created on: 2015-11-23
 * Since version: 0.2.0
 */
public class MonarchSplitTest extends TestBase {
  @DataProvider
  public static Object[][] getData() {
    return new Object[][]{
      //// input:
      //    - split-path
      //    - split-size
      //    - total-count,
      //// assert:
      //    - expected-number-of-splits
      //    - size-of-last-split
      //
      {new Path("/dummy"),  100,  31,   1,    31},
      {new Path("/dummy"),  10,   31,   4,    1},
      {new Path("/dummy"),  10,   310,  31,   10},
    };
  }

  /**
   * Test for number of input splits for different number of records and split sizes.
   *   Assert following is correct:
   *    - total number of splits
   *    - size of each split, except last
   *    - size of last split which may be less than split size
   *
   * @param path the path to the split file (dummy in this case)
   * @param splitSize the split size (max number of records in each split)
   * @param count the total number of records
   * @param expectedSplitCount expected number of splits
   * @param expectedLastSplitLength expected number of records in last split
   */
  @Test(dataProvider = "getData")
  public void testGetInputSplits(final Path path, final long splitSize, final int count,
                                 final int expectedSplitCount, final long expectedLastSplitLength) {
    MonarchSplit[] mss = MonarchSplit.getInputSplits(path, "dummy", splitSize, count);

    assertEquals(mss.length, expectedSplitCount);
    for (int i = 0; i < mss.length - 1; ++i) {
      assertEquals(mss[i].getLength(), splitSize);
    }
    assertEquals(mss[mss.length-1].getLength(), expectedLastSplitLength);
  }

  /**
   * Test for number of splits with different number of blocks with multiple
   *    key-prefix and variable block sizes.
   * Single test for multiple combinations so that VM creation can be optimized.
   *    Same DUnit VMs can be used for multiple inputs.
   * @throws Exception
   */
  @Test
  public void testGetSplits() throws Exception {
    /** map of key-prefix and total-blocks for the prefix and length of individual
     *   split in case the block-size is greater than split-size
     */
    Map<String, Long[]> map = new HashMap<>(1);

    final String regionName = "region_split_test";

    testBase.createRegionOnServer(regionName+MonarchUtils.META_TABLE_SFX);

    /** no blocks in the region.. returns array of zero length **/
    assertGetSplits(testBase, regionName, map, 2);

    map.put("0-", new Long[] {0l, 0l});
    assertGetSplits(testBase, regionName, map, 2);

    /** more blocks, for a prefix, than split-size..
     *  returns multiple splits each with max-length up to split-size **/
    map.put("0-", new Long[] {10l, 2l, 2l, 2l, 2l, 2l});
    assertGetSplits(testBase, regionName, map, 2);

    /** same as above but shorter length for last split **/
    map.put("0-", new Long[] {9l, 2l, 2l, 2l, 2l, 1l});
    assertGetSplits(testBase, regionName, map, 2);

    /** split-size larger than number of blocks.. returns single split **/
    map.put("0-", new Long[] {9l, 9l});
    assertGetSplits(testBase, regionName, map, 100);

    /** multiple prefix with less number of blocks than split size **/
    map.put("0-", new Long[] {1l, 1l});
    map.put("1-", new Long[] {1l, 1l});
    assertGetSplits(testBase, regionName, map, 2);

    map.put("0-", new Long[] {5l, 2l, 2l, 1l});
    map.put("1-", new Long[] {8l, 2l, 2l, 2l, 2l});
    assertGetSplits(testBase, regionName, map, 2);

    testBase.destroyRegionOnServer(regionName);
    Configuration conf = new Configuration();
    conf.set("monarch.locator.port", testBase.getLocatorPort());
  }

  /**
   * Assert that the splits are created as expected from the specified number of blocks.
   *   First puts the required key-values in a region and then queries the
   *   required splits.
   *
   * @param mtb the test helper for monarch interactions
   * @param regionName the region name
   * @param map the map containing prefix and block-count with expected length for splits
   * @param splitSize the split size
   * @throws RMIException
   */
  private void assertGetSplits(final MonarchDUnitBase mtb, final String regionName,
                               final Map<String, Long[]> map, final long splitSize) throws RMIException {
    int expectedSplitCount = 0;
    Long[] value;
    /** MonarchSplit.getSplits looks for the data in region with suffix..
     *   so need to put data in region with correct name **/
    for (Map.Entry<String, Long[]> e : map.entrySet()) {
      value = e.getValue();
      mtb.putInRegionOnServer(regionName + MonarchUtils.META_TABLE_SFX, e.getKey() + MonarchUtils.KEY_BLOCKS_SFX, value[0]);
      mtb.assertOnClient(regionName + MonarchUtils.META_TABLE_SFX, e.getKey() + MonarchUtils.KEY_BLOCKS_SFX, value[0]);
      expectedSplitCount += (value.length - 1);
    }

    /** setup the job-configuration and get the required splits **/
    JobConf jobConf = new JobConf();
    jobConf.set(MonarchUtils.REGION, regionName);
    jobConf.set(MonarchUtils.LOCATOR_PORT, mtb.getLocatorPort());
    jobConf.set(MonarchUtils.SPLIT_SIZE_KEY, String.valueOf(splitSize));
    jobConf.set("mapred.input.dir", "dummy");

    MonarchSplit[] sps = (MonarchSplit[])MonarchSplit.getSplits(jobConf, 1, 0);
    assertEquals(sps.length, expectedSplitCount);

//    System.out.println("MonarchSplits = " + Arrays.asList(sps));

    /** assert on length of individual splits **/
    Map<String, Integer> expectedSplitLengthIndexMap = new HashMap<>(5);
    Integer posInSamePrefix;
    long expectedLength;
    for (final MonarchSplit sp : sps) {
      posInSamePrefix = expectedSplitLengthIndexMap.get(sp.getKeyPrefix());
      if (posInSamePrefix == null) {
        posInSamePrefix = 1;
      }
      expectedLength = map.get(sp.getKeyPrefix())[posInSamePrefix++];
      assertEquals(sp.getLength(), expectedLength);
      expectedSplitLengthIndexMap.put(sp.getKeyPrefix(), posInSamePrefix);
    }
  }
}