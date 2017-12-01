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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;

import java.util.Objects;

public class AmpoolTableLayoutHandle implements ConnectorTableLayoutHandle
{
    private static final Logger log = Logger.get(AmpoolTableLayoutHandle.class);

    private final AmpoolTableHandle table;

    @JsonCreator
    public AmpoolTableLayoutHandle(@JsonProperty("table") AmpoolTableHandle table)
    {
        this.table = table;
        log.info("INFORMATION: AmpoolTableLayoutHandle created successfully.");
    }

    @JsonProperty
    public AmpoolTableHandle getTable()
    {
        log.info("INFORMATION: AmpoolTableLayoutHandle getTable() called.");
        return table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AmpoolTableLayoutHandle that = (AmpoolTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
