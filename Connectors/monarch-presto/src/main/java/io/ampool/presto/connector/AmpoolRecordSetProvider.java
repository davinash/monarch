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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AmpoolRecordSetProvider implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(AmpoolRecordSetProvider.class);

    private final String connectorId;
    private final AmpoolClient ampoolClient;

    @Inject
    public AmpoolRecordSetProvider(AmpoolConnectorID connectorId, AmpoolClient ampoolClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.ampoolClient = requireNonNull(ampoolClient, "ampoolClient is null");
        log.info("INFORMATION: AmpoolRecordSetProvider created successfully.");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle connectorTransactionHandle, ConnectorSession connectorSession, ConnectorSplit connectorSplit, List<? extends ColumnHandle> list)
    {
        log.info("INFORMATION: AmpoolRecordSetProvider getRecordSet() called.");

        requireNonNull(connectorSplit, "split is null");
        AmpoolSplit ampoolSplit = (AmpoolSplit) connectorSplit;
        checkArgument(ampoolSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<AmpoolColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : list)
        {
            handles.add((AmpoolColumnHandle) handle);
        }


        // TODO: Projections and filters on Ampool side
        Iterator<Row> iterator;
        if (ampoolClient.existsFTable(ampoolSplit.getTableName()))
            iterator = ampoolClient.getFTable(ampoolSplit.getTableName()).getScanner(new Scan()).iterator();
        else if (ampoolClient.existsMTable(ampoolSplit.getTableName()))
            iterator = ampoolClient.getMTable(ampoolSplit.getTableName()).getScanner(new Scan()).iterator();
        else
            iterator = null;

        return new AmpoolRecordSet(ampoolSplit, handles.build(), iterator);
    }
}
