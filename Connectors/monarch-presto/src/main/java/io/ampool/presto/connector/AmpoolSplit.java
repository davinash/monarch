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

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

public class AmpoolSplit implements ConnectorSplit
{
    private static final Logger log = Logger.get(AmpoolSplit.class);

    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String bucketId;
    private final HostAddress address;

    @JsonCreator
    public AmpoolSplit(@JsonProperty("connectorId") String connectorId,
                       @JsonProperty("schemaName") String schemaName,
                       @JsonProperty("tableName") String tableName,
                       @JsonProperty("bucketId") String bucketId,
                       @JsonProperty("address") HostAddress address)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.bucketId = requireNonNull(bucketId, "bucket id is null");
        this.address = requireNonNull(address, "address is null");
        log.info("INFORMATION: AmpoolSplit created successfully.");
    }

    @JsonProperty
    public String getConnectorId()
    {
        log.info("INFORMATION: AmpoolSplit getConnectorId() called.");
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        log.info("INFORMATION: AmpoolSplit getSchemaName() called.");
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        log.info("INFORMATION: AmpoolSplit getTableName() called.");
        return tableName;
    }

    @JsonProperty
    public String getBucketId()
    {
        log.info("INFORMATION: AmpoolSplit getBucketId() called.");
        return bucketId;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        log.info("INFORMATION: AmpoolSplit getAddress() called.");
        return address;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        log.info("INFORMATION: AmpoolSplit isRemotelyAccessible() called.");
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        log.info("INFORMATION: AmpoolSplit getAddresses() called.");
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        log.info("INFORMATION: AmpoolSplit getInfo() called.");
        return this;
    }
}
