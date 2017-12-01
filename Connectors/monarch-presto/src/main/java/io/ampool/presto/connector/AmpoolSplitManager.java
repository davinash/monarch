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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;

public class AmpoolSplitManager implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(AmpoolSplitManager.class);

    private final String connectorId;
    private final AmpoolClient ampoolClient;

    @Inject
    public AmpoolSplitManager(AmpoolConnectorID connectorId, AmpoolClient ampoolClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.ampoolClient = requireNonNull(ampoolClient, "client is null");
        log.info("INFORMATION: AmpoolSplitManager created successfully.");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        log.info("INFORMATION: AmpoolSplitManager getSplits() called.");

        AmpoolTableLayoutHandle layoutHandle = (AmpoolTableLayoutHandle) layout;
        AmpoolTableHandle tableHandle = layoutHandle.getTable();
        AmpoolTable table = new AmpoolTable(ampoolClient, tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table.getColumnsMetadata() != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        // TODO Pass here bucket id
        splits.add(new AmpoolSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(),"" ,HostAddress.fromParts("localhost",0)));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
