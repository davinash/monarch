/*
* Copyright (c) 2017 Ampool, Inc. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License. See accompanying
* LICENSE file.
*/
package io.ampool.presto.connector;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.ampool.monarch.table.Row;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AmpoolRecordSet implements RecordSet
{
    private static final Logger log = Logger.get(AmpoolRecordSet.class);

    private final List<AmpoolColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Iterator<Row> iterator;

    public AmpoolRecordSet(AmpoolSplit split, List<AmpoolColumnHandle> columnHandles, Iterator<Row> iterator)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AmpoolColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.iterator = iterator;

        log.info("INFORMATION: AmpoolRecordSet created successfully.");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        log.info("INFORMATION: AmpoolRecordSet getColumnTypes() called.");
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        log.info("INFORMATION: AmpoolRecordSet cursor() called.");
        return new AmpoolRecordCursor(columnHandles, iterator);
    }
}
