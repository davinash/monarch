package io.ampool.monarch.table.facttable.dunit;

import org.apache.geode.test.junit.categories.FTableTest;

import org.junit.experimental.categories.Category;

/**
 * Tests ftable scan with overflow enabled so records will go to next tier. The overflow is at 1.0
 * so all records will go to the tier This test verifies if scan is working fine from stores
 */
@Category(FTableTest.class)
public class FTableServerScanOverflowDUnitTest extends FTableServerScanTests {
  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    setEvictionPercetangeOnAllVMs(1);
  }
}


