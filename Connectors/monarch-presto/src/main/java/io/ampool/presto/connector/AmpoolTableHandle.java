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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class AmpoolTableHandle implements ConnectorTableHandle
{
    private static final Logger log = Logger.get(AmpoolTableHandle.class);

    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public AmpoolTableHandle(@JsonProperty("connectorId") String connectorId,
                             @JsonProperty("schemaName") String schemaName,
                             @JsonProperty("tableName")String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        log.info("INFORMATION: AmpoolTableHandle created successfully.");
    }

    @JsonProperty
    public String getConnectorId()
    {
        log.info("INFORMATION: AmpoolTableHandle getConnectorId() called.");
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        log.info("INFORMATION: AmpoolTableHandle getSchemaName() called.");
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        log.info("INFORMATION: AmpoolTableHandle getTableName() called.");
        return tableName;
    }

    public SchemaTableName toSchemaTableName()
    {
        log.info("INFORMATION: AmpoolTableHandle toSchemaTableName() called.");
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AmpoolTableHandle other = (AmpoolTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaName, tableName);
    }
}
