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
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;

public class AmpoolHandleResolver implements ConnectorHandleResolver
{
    private static final Logger log = Logger.get(AmpoolHandleResolver.class);

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        log.info("INFORMATION: AmpoolHandleResolver getTableLayoutHandleClass() called.");
        return AmpoolTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        log.info("INFORMATION: AmpoolHandleResolver getTableHandleClass() called.");
        return AmpoolTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        log.info("INFORMATION: AmpoolHandleResolver getColumnHandleClass() called.");
        return AmpoolColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        log.info("INFORMATION: AmpoolHandleResolver getSplitClass() called.");
        return AmpoolSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        log.info("INFORMATION: AmpoolHandleResolver getTransactionHandleClass() called.");
        return AmpoolTransactionHandle.class;
    }
}
