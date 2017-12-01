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

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static io.ampool.presto.connector.AmpoolTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class AmpoolConnector implements Connector
{
    private static final Logger log = Logger.get(AmpoolConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final AmpoolMetadata metadata;
    private final AmpoolSplitManager splitManager;
    private final AmpoolRecordSetProvider recordSetProvider;

    @Inject
    public AmpoolConnector(
            LifeCycleManager lifeCycleManager,
            AmpoolMetadata metadata,
            AmpoolSplitManager splitManager,
            AmpoolRecordSetProvider recordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        log.info("INFORMATION: AmpoolConnector created.");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b)
    {
        log.info("INFORMATION: AmpoolConnector beginTransaction() called.");
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        log.info("INFORMATION: AmpoolConnector getMetadata() called.");
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        log.info("INFORMATION: AmpoolConnector getSplitManager() called.");
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        log.info("INFORMATION: AmpoolConnector getRecordSetProvider() called.");
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
            log.info("INFORMATION: AmpoolConnector shutdown() called.");
        }
        catch (Exception e) {
            log.info("ERROR: " + e.getMessage());
        }
    }
}
