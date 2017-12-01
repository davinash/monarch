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

package io.ampool.tierstore.internal;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.ConverterDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * A converter descriptor for Ampool to parquet (through avro)
 */
public class ParquetConverterDescriptor implements ConverterDescriptor {

  private List<ColumnConverterDescriptor> columnConverters = new ArrayList<>();

  public ParquetConverterDescriptor(TableDescriptor tableDescriptor) {
    for (final MColumnDescriptor mColumnDescriptor : tableDescriptor.getAllColumnDescriptors()) {
      ColumnConverterDescriptor columnConvertorDescriptor = new ParquetColumnConverterDescriptor(
          mColumnDescriptor.getColumnType(), mColumnDescriptor.getColumnNameAsString());
      columnConverters.add(columnConvertorDescriptor);
    }
  }

  public List<ColumnConverterDescriptor> getColumnConverters() {
    return columnConverters;
  }
}
