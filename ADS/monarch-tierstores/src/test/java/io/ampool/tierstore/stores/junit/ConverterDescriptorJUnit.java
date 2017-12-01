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

import java.util.List;

import org.apache.geode.test.junit.categories.StoreTest;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


@Category(StoreTest.class)
public class ConverterDescriptorJUnit {

  @Rule
  public TestName name = new TestName();

  @Test
  public void testSimpleTypes() {
    final ConverterDescriptor converterDescriptor =
        StoreTestUtils.getConverterDescriptor("testSimpleTypes", true, FileFormats.ORC);
    final StoreRecord storeRecord = StoreTestUtils.getStoreRecord();
    final Object[] values = storeRecord.getValues();
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    final Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      final Object writable = columnConverters.get(i).getWritable(values[i]);
      System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
      convertedValues[i] = columnConverters.get(i).getReadable(writable);
      System.out.println(
          "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
    }
    StoreTestUtils.validateConvertedValues(convertedValues);
  }

  @Test
  public void testSimpleTypesForParquet() {
    final ConverterDescriptor converterDescriptor =
        StoreTestUtils.getConverterDescriptor(name.getMethodName(), true, FileFormats.PARQUET);
    final StoreRecord storeRecord = StoreTestUtils.getStoreRecord();
    final Object[] values = storeRecord.getValues();
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    final Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      final Object writable = columnConverters.get(i).getWritable(values[i]);
      System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
      convertedValues[i] = columnConverters.get(i).getReadable(writable);
      System.out.println(
          "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
    }
    StoreTestUtils.validateConvertedValues(convertedValues);
  }

  @Test
  public void testComplexTypes() {
    final ConverterDescriptor converterDescriptor =
        StoreTestUtils.getConverterDescriptorComplexTypes("testComplexTypes", FileFormats.ORC);
    final StoreRecord storeRecord = StoreTestUtils.getStoreRecordWithComplexTypes();
    final Object[] values = storeRecord.getValues();
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    final Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      final Object writable = columnConverters.get(i).getWritable(values[i]);
      if (writable != null) {
        System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
      }
      convertedValues[i] = columnConverters.get(i).getReadable(writable);
      if (convertedValues[i] != null) {
        System.out.println(
            "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
      }
    }
    // StoreTestUtils.validateConvertedValues(convertedValues);
  }

  @Test
  public void testComplexTypesForParquet() {
    final ConverterDescriptor converterDescriptor = StoreTestUtils
        .getConverterDescriptorComplexTypes(name.getMethodName(), FileFormats.PARQUET);
    final StoreRecord storeRecord = StoreTestUtils.getStoreRecordWithComplexTypes();
    final Object[] values = storeRecord.getValues();
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    final Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      final Object writable = columnConverters.get(i).getWritable(values[i]);
      if (writable != null) {
        System.out.println("Writable type : " + writable.getClass() + " Value : " + writable);
      }
      convertedValues[i] = columnConverters.get(i).getReadable(writable);
      if (convertedValues[i] != null) {
        System.out.println(
            "Readable type : " + convertedValues[i].getClass() + " Value : " + convertedValues[i]);
      }
    }
    // StoreTestUtils.validateConvertedValues(convertedValues);
  }

}
