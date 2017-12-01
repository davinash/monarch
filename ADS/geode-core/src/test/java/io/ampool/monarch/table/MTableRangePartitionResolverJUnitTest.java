package io.ampool.monarch.table;

import static org.junit.Assert.fail;

import java.util.Hashtable;
import java.util.Map;
import java.util.Random;

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MTableRangePartitionResolver;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Created by rgeiger on 16/2/24.
 */
@Category(MonarchTest.class)
public class MTableRangePartitionResolverJUnitTest extends MTableDUnitHelper {
  public MTableRangePartitionResolverJUnitTest() {
    super();
  }

  @Override
  public void preSetUp() throws Exception {}

  @Override
  public void tearDown2() throws Exception {}

  @Test
  public void testSplits() {
    byte[] rangeStart = new byte[] {(byte) 0x00};
    byte[] rangeStop = new byte[] {(byte) 0xFF};

    // should be OK
    MTableRangePartitionResolver res =
        new MTableRangePartitionResolver(255, rangeStart, rangeStop, null);

    // should fail
    try {
      res = new MTableRangePartitionResolver(512, rangeStart, rangeStop, null);
    } catch (IllegalArgumentException exc) {
      fail("excessive splits test failed");
    }

    rangeStart = new byte[] {(byte) 0x02, (byte) 0x00};
    rangeStop = new byte[] {(byte) 0xFE, (byte) 0x00};;

    // should be OK
    res = new MTableRangePartitionResolver(512, rangeStart, rangeStop, null);

    // should fail
    try {
      res = new MTableRangePartitionResolver(65100, rangeStart, rangeStop, null);
    } catch (IllegalArgumentException exc) {
      fail("excessive splits test failed");
    }
  }

  @Test
  public void testPlaceKeysRange() {
    byte[] rangeStart = new byte[] {(byte) 0x00};
    byte[] rangeStop = new byte[] {(byte) 0xFF};

    Hashtable<Object, Integer> ranges = new Hashtable<Object, Integer>();

    MTableRangePartitionResolver res =
        new MTableRangePartitionResolver(4, rangeStart, rangeStop, null);

    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : res.getSplitsMap().entrySet()) {
      ranges.put(entry.getKey(), new Integer(0));
    }

    long longKey;
    for (int i = 0; i < 256; i++) {
      longKey = Long.reverseBytes(i);
      byte[] key = java.nio.ByteBuffer.allocate(8).putLong(longKey).array();
      Object entry = res.getInternalRO(key);
      if (entry != null) {
        Integer count = ranges.get(entry);
        ranges.put(entry, count.intValue() + 1);
      } else {
        fail("FAILED TO MAP A KEY");
      }
    }

    int prevCount = -1;
    for (Integer count : ranges.values()) {
      if (prevCount != -1) {
        // perfect distribution so no more than a delta of 2
        if (Math.abs(count - prevCount) > 2) {
          fail("uneven bucket distribution");
        }
      }
      prevCount = count;
    }
  }

  @Test
  public void testPlaceKeysPartialRange() {
    // Test to insure out of range keys are not mapped when a partial range is configured
    byte[] rangeStart = new byte[] {(byte) 0x04};
    byte[] rangeStop = new byte[] {(byte) 0xFF};

    Hashtable<Object, Integer> ranges = new Hashtable<Object, Integer>();

    MTableRangePartitionResolver res =
        new MTableRangePartitionResolver(4, rangeStart, rangeStop, null);

    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : res.getSplitsMap().entrySet()) {
      ranges.put(entry.getKey(), new Integer(0));
    }

    long longKey;
    for (int i = 0; i < 256; i++) {
      longKey = Long.reverseBytes(i);
      byte[] key = java.nio.ByteBuffer.allocate(8).putLong(longKey).array();
      Object entry = res.getInternalRO(key);
      if (entry != null) {
        if ((i & 0xFF) < 4) {
          fail("MAPPED OUT OF RANGE KEY");
        }
        Integer count = ranges.get(entry);
        ranges.put(entry, count.intValue() + 1);
      } else {
        if ((i & 0xFF) > 4) {
          fail("FAILED TO MAP A KEY");
        }
      }
    }

    int prevCount = -1;
    for (Integer count : ranges.values()) {
      if (prevCount != -1) {
        // perfect distribution so no more than a delta of 2
        if (Math.abs(count - prevCount) > 2) {
          fail("uneven bucket distribution");
        }
      }
      prevCount = count;
    }
  }

  @Test
  public void testPlaceIntKeysPartialRange() {
    // Test to insure integer keys are handled properly.

    // get start, stop as user would in creating a table
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setStartStopRangeKey(0, 100);
    byte[] rangeStart = tableDescriptor.getStartRangeKey();
    byte[] rangeStop = tableDescriptor.getStopRangeKey();

    Hashtable<Object, Integer> ranges = new Hashtable<Object, Integer>();

    MTableRangePartitionResolver res =
        new MTableRangePartitionResolver(10, rangeStart, rangeStop, null);

    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : res.getSplitsMap().entrySet()) {
      ranges.put(entry.getKey(), new Integer(0));
    }

    for (int i = 0; i < 120; i++) {
      byte[] key = MTableUtils.integerToKey(i);
      Object entry = res.getInternalRO(key);
      if (entry != null) {
        if (i > 100) {
          fail("MAPPED OUT OF RANGE KEY");
        }
        Integer count = ranges.get(entry);
        ranges.put(entry, count.intValue() + 1);
      } else {
        if (i <= 100) {
          fail("FAILED TO MAP A KEY");
        }
      }
    }

    int prevCount = -1;
    for (Integer count : ranges.values()) {
      if (prevCount != -1) {
        // perfect distribution so no more than a delta of 2
        if (Math.abs(count - prevCount) > 2) {
          fail("uneven bucket distribution");
        }
      }
      prevCount = count;
    }
  }

  @Test
  public void testPlaceLongKeysPartialRange() {
    // Test to insure long integer keys are handled properly

    // get start, stop as user would in creating a table
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setStartStopRangeKey(0L, 100L);
    byte[] rangeStart = tableDescriptor.getStartRangeKey();
    byte[] rangeStop = tableDescriptor.getStopRangeKey();

    Hashtable<Object, Integer> ranges = new Hashtable<Object, Integer>();

    MTableRangePartitionResolver res =
        new MTableRangePartitionResolver(10, rangeStart, rangeStop, null);

    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : res.getSplitsMap().entrySet()) {
      ranges.put(entry.getKey(), new Integer(0));
    }

    for (int i = 0; i < 120; i++) {
      byte[] key = MTableUtils.longIntegerToKey(i);
      Object entry = res.getInternalRO(key);
      if (entry != null) {
        if (i > 100) {
          fail("MAPPED OUT OF RANGE KEY");
        }
        Integer count = ranges.get(entry);
        ranges.put(entry, count.intValue() + 1);
      } else {
        if (i <= 100) {
          fail("FAILED TO MAP A KEY");
        }
      }
    }

    int prevCount = -1;
    for (Integer count : ranges.values()) {
      if (prevCount != -1) {
        // perfect distribution so no more than a delta of 2
        if (Math.abs(count - prevCount) > 2) {
          fail("uneven bucket distribution");
        }
      }
      prevCount = count;
    }
  }

  @Test
  public void testPlaceKeys() {
    Hashtable<Object, Integer> ranges = new Hashtable<Object, Integer>();

    Random random = new Random();

    // use the default range
    MTableRangePartitionResolver res = new MTableRangePartitionResolver(113);

    // set counts to 0
    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : res.getSplitsMap().entrySet()) {
      ranges.put(entry.getKey(), new Integer(0));
    }

    int numItems = 10000;

    for (int i = 0; i < numItems; i++) {
      long longKey = Long.reverseBytes(random.nextLong());
      byte[] key = java.nio.ByteBuffer.allocate(8).putLong(longKey).array();
      Object entry = res.getInternalRO(key);
      if (entry != null) {
        Integer count = ranges.get(entry);
        ranges.put(entry, count.intValue() + 1);
      } else {
        fail("FAILED TO MAP A KEY");
      }
    }

    int largest = -1, smallest = Integer.MAX_VALUE;
    for (Integer count : ranges.values()) {
      if (count > largest) {
        largest = count;
      }
      if (count < smallest) {
        smallest = count;
      }
    }
    if ((((largest - smallest) * 100) / numItems) >= 1) {
      fail("uneven bucket distribution");
    }
  }
}
