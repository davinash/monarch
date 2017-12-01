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
package io.ampool.monarch.table.internal;

import static io.ampool.monarch.table.ftable.FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.apache.geode.test.junit.categories.FTableTest;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@Category(FTableTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class MAbstractResultScannerTest {
  private static final ProxyFTableRegion table;
  private static final FTableDescriptor descriptor;
  public static final MClientScannerUsingGetAll scanner;

  public static final byte[] PARTITIONING_COLUMN = "c_0".getBytes(Charset.forName("UTF-8"));

  static {
    /** mock the table-descriptor **/
    descriptor = Mockito.mock(FTableDescriptor.class);
    Mockito.when(descriptor.getNumOfColumns()).thenReturn(10);
    Mockito.when(descriptor.getTotalNumOfSplits()).thenReturn(100);
    Mockito.when(descriptor.getPartitioningColumn())
        .thenReturn(new ByteArrayKey(PARTITIONING_COLUMN));

    /** mock the table **/
    table = Mockito.mock(ProxyFTableRegion.class);
    Mockito.when(table.getName()).thenReturn(null);
    Mockito.when(table.getTableDescriptor()).thenReturn(descriptor);

    /** mock the scanner **/
    scanner = Mockito.mock(MClientScannerUsingGetAll.class);
    Mockito.when(scanner.getApplicableBucketIds(anyObject(), anyObject())).thenCallRealMethod();
  }

  /** parameters for executing the test **/
  @Parameterized.Parameter(0)
  public String message;
  @Parameterized.Parameter(1)
  public int expectedCount;
  @Parameterized.Parameter(2)
  public Filter filter;

  @Parameterized.Parameters
  public static Collection<Object> data() {
    /** each parameter has: description, expected-bucket-count, scan-filters **/
    return Arrays
        .asList(new Object[] {new Object[] {"No Filters", descriptor.getTotalNumOfSplits(), null},
            new Object[] {"Single Filter with Partitioning Column - EQUALS", 1,
                new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, null)},
            new Object[] {"Multiple OR Filters with Partitioning Column - EQUALS",
                descriptor.getTotalNumOfSplits(), new FilterList(FilterList.Operator.MUST_PASS_ONE)
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_1"))
                    .addFilter(
                        new SingleColumnValueFilter("c_1".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Single OR Filter with Partitioning Column - EQUALS", 1,
                new FilterList(FilterList.Operator.MUST_PASS_ONE).addFilter(
                    new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_1"))},
            new Object[] {"Multiple AND Filters with Partitioning Column - EQUALS", 1,
                new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_0"))
                    .addFilter(
                        new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Multiple AND Filters with Partitioning Column without EQUALS",
                descriptor.getTotalNumOfSplits(), new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(new SingleColumnValueFilter(PARTITIONING_COLUMN,
                        CompareOp.GREATER_OR_EQUAL, "junk_0"))
                    .addFilter(
                        new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Multiple AND Filters without Partitioning Column",
                descriptor.getTotalNumOfSplits(), new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(
                        new SingleColumnValueFilter("c_1".getBytes(), CompareOp.EQUAL, "junk_1"))
                    .addFilter(
                        new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Multiple AND Filters with Partitioning Column more than Once", 3,
                new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_0"))
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_1"))
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_2"))
                    .addFilter(
                        new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Multiple AND Filters with Partitioning Column Repeating Value", 2,
                new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_0"))
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_1"))
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_0"))
                    .addFilter(
                        new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL, "junk_2"))},
            new Object[] {"Multiple AND Filters with Partitioning Column and INSERTION_TIME", 1,
                new FilterList(FilterList.Operator.MUST_PASS_ALL)
                    .addFilter(
                        new SingleColumnValueFilter(PARTITIONING_COLUMN, CompareOp.EQUAL, "junk_0"))
                    .addFilter(new SingleColumnValueFilter(INSERTION_TIMESTAMP_COL_NAME,
                        CompareOp.GREATER_OR_EQUAL, 0L))
                    .addFilter(new SingleColumnValueFilter(INSERTION_TIMESTAMP_COL_NAME,
                        CompareOp.LESS_OR_EQUAL, Long.MAX_VALUE))
                    .addFilter(new SingleColumnValueFilter("c_2".getBytes(), CompareOp.EQUAL,
                        "junk_2"))},});
  }

  /**
   * Assert that the expected number of buckets are used by the scanner in various scenarios.
   *
   * @throws Exception
   */
  @Test
  public void testGetApplicableBucketIds() throws Exception {
    System.out.println(this.message + " :: expectedBucketCount= " + this.expectedCount);
    Scan scan = new Scan();
    scan.setFilter(this.filter);
    Set<Integer> buckets = scanner.getApplicableBucketIds(scan, descriptor);
    assertEquals("Incorrect number of buckets in scan.", this.expectedCount, buckets.size());
  }
}
