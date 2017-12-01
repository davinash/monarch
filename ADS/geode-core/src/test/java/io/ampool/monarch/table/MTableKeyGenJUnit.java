package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.internal.MTableUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(MonarchTest.class)
public class MTableKeyGenJUnit extends MTableDUnitHelper {
  public MTableKeyGenJUnit() {
    super();
  }

  @Override
  public void preSetUp() throws Exception {}

  @Override
  public void tearDown2() throws Exception {}


  @Test
  public void testKeyGeneration() {

    Map<Integer, List<byte[]>> keysForAllBuckets = getKeysForAllBuckets(113, 100);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });


    Map<Integer, Pair<byte[], byte[]>> uniformSplits = MTableUtils.getUniformKeySpaceSplit(113);
    Map<Integer, Integer> partitionToHits = new HashMap<>();

    allKeys.forEach((K) -> {
      uniformSplits.forEach((KEY, V) -> {
        if ((Bytes.compareTo(K, V.getFirst()) >= 0) && (Bytes.compareTo(K, V.getSecond()) < 0)) {
          partitionToHits.put(KEY,
              partitionToHits.get(KEY) == null ? 0 : partitionToHits.get(KEY) + 1);
        }
      });
    });
    partitionToHits.forEach((K, V) -> assertEquals(99, (int) V));
  }
}
