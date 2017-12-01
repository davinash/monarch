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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;

import java.util.List;

public class AmpoolTable
{
    private static final Logger log = Logger.get(AmpoolTable.class);

    private final AmpoolClient ampoolClient;
    private final String name;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public AmpoolTable(@JsonProperty("ampoolClient") AmpoolClient ampoolClient,
                       @JsonProperty("name")String name)
    {
        this.ampoolClient = ampoolClient;
        this.name = name;

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        if (ampoolClient.getAdmin().existsFTable(name))
        {
            FTable table = ampoolClient.getFTable(name);
            for (MColumnDescriptor column : table.getTableDescriptor().getAllColumnDescriptors())
            {
                columnsMetadata.add(new ColumnMetadata(column.getColumnNameAsString(), mapType(column.getColumnType())));
            }
            this.columnsMetadata = columnsMetadata.build();
            log.info("INFORMATION: AmpoolFTable created successfully.");
        }
        else if (ampoolClient.getAdmin().existsMTable(name))
        {
            MTable table = ampoolClient.getMTable(name);
            for (MColumnDescriptor column : table.getTableDescriptor().getAllColumnDescriptors())
            {
                columnsMetadata.add(new ColumnMetadata(column.getColumnNameAsString(), mapType(column.getColumnType())));
            }
            this.columnsMetadata = columnsMetadata.build();
            log.info("INFORMATION: AmpoolMTable created successfully.");
        }
        else
        {
            this.columnsMetadata = null;
            log.info("INFORMATION: AmpoolTable created successfully, but didn't load any data.");
        }

    }

    @JsonProperty
    public AmpoolClient getClient()
    {
        return ampoolClient;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        log.info("INFORMATION: AmpoolTable getColumnsMetadata() called.");
        return columnsMetadata;
    }

    public static Type mapType(DataType ampoolType)
    {
        log.info("INFORMATION: AmpoolTable mapType() called.");

        if (ampoolType == BasicTypes.STRING || ampoolType == BasicTypes.VARCHAR)
            return VarcharType.VARCHAR;
        else if (ampoolType == BasicTypes.BINARY)
            return VarbinaryType.VARBINARY;
        else if (ampoolType == BasicTypes.INT)
            return IntegerType.INTEGER;
        else if (ampoolType == BasicTypes.LONG)
            return BigintType.BIGINT;
        else if (ampoolType == BasicTypes.DOUBLE)
            return DoubleType.DOUBLE;
        else if (ampoolType == BasicTypes.FLOAT)
            return RealType.REAL;
        else if (ampoolType == BasicTypes.DATE)
            return DateType.DATE;
        else if (ampoolType == BasicTypes.TIMESTAMP)
            return TimestampType.TIMESTAMP;
        else
        {
            log.info("ERROR: Type "
                    + ampoolType.getCategory().name()
                    + " not recognized.");
            return null;
        }
    }
}