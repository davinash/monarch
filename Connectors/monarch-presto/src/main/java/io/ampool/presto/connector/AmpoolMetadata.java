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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;
import org.apache.commons.lang.ArrayUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AmpoolMetadata implements ConnectorMetadata
{
    private static final Logger log = Logger.get(AmpoolMetadata.class);

    private final String connectorId;
    private final AmpoolClient ampoolClient;

    @Inject
    public AmpoolMetadata(AmpoolConnectorID connectorId, AmpoolClient ampoolClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null.").toString();
        this.ampoolClient = requireNonNull(ampoolClient, "ampoolClient is null");
        log.info("INFORMATION: AmpoolMetadata created successfully.");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        log.info("INFORMATION: AmpoolMetadata listSchemaNames() called.");
        return ImmutableList.of("ampool");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        log.info("INFORMATION: AmpoolMetadata getTableHandle() called. Looking for table "+schemaTableName.getSchemaName()+" | "+schemaTableName.getTableName());

        AmpoolTable table = new AmpoolTable(ampoolClient, schemaTableName.getTableName());
        if (table.getColumnsMetadata() == null) {
            log.info("No column meta...");
            return null;
        }

        return new AmpoolTableHandle(connectorId, "ampool", schemaTableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> optional)
    {
        log.info("INFORMATION: AmpoolMetadata getTableLayouts() called.");

        AmpoolTableHandle tableHandle = (AmpoolTableHandle) connectorTableHandle;
        ConnectorTableLayout layout = new ConnectorTableLayout(new AmpoolTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession connectorSession, ConnectorTableLayoutHandle connectorTableLayoutHandle)
    {
        log.info("INFORMATION: AmpoolMetadata getTableLayout() called.");
        return new ConnectorTableLayout(connectorTableLayoutHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        log.info("INFORMATION: AmpoolMetadata getTableMetadata() called.");

        AmpoolTableHandle exampleTableHandle = (AmpoolTableHandle) connectorTableHandle;
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());

        AmpoolTable table = new AmpoolTable(ampoolClient, tableName.getTableName());
        if (table.getColumnsMetadata() == null)
            return null;

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession connectorSession, String s)
    {
        log.info("INFORMATION: AmpoolMetadata listTables() called.");

        if (s == null) {
            return listTables(connectorSession, "ampool");
        }

        String[] fTableNames = ampoolClient.getAdmin().listFTableNames();
        String[] mTableNames = ampoolClient.getAdmin().listMTableNames();
        String[] tableNames = (String[]) ArrayUtils.addAll(fTableNames, mTableNames);
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String n : tableNames)
        {
            tables.add(new SchemaTableName("ampool", n));
        }

        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        log.info("INFORMATION: AmpoolMetadata getColumnHandles() called.");

        AmpoolTableHandle ampoolTableHandle = (AmpoolTableHandle) connectorTableHandle;
        checkArgument(ampoolTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        AmpoolTable table = new AmpoolTable(ampoolClient, ampoolTableHandle.getTableName());
        if (table.getColumnsMetadata() == null)
            throw new TableNotFoundException(ampoolTableHandle.toSchemaTableName());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new AmpoolColumnHandle(connectorId, column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle, ColumnHandle columnHandle)
    {
        log.info("INFORMATION: AmpoolMetadata getColumnMetadata() called.");
        return ((AmpoolColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession connectorSession, SchemaTablePrefix schemaTablePrefix)
    {
        log.info("INFORMATION: AmpoolMetadata listTableColumns() called.");

        requireNonNull(schemaTablePrefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName schemaTableName : listTables(connectorSession, schemaTablePrefix.getSchemaName()))
        {
            AmpoolTableHandle tableHandle = new AmpoolTableHandle(connectorId, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            ConnectorTableMetadata tableMetadata = getTableMetadata(connectorSession, tableHandle);
            if (tableMetadata != null)
                columns.put(schemaTableName, tableMetadata.getColumns());
        }

        return columns.build();
    }
}
