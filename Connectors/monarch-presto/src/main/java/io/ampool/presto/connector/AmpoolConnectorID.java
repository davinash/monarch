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

import io.airlift.log.Logger;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class AmpoolConnectorID
{
    private static final Logger log = Logger.get(AmpoolConnectorID.class);

    private final String id;

    public AmpoolConnectorID(String id)
    {
        this.id = requireNonNull(id, "id is null");
        log.info("INFORMATION: AmpoolConnectorID created successfully.");
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
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

        AmpoolConnectorID other = (AmpoolConnectorID) obj;
        return Objects.equals(this.id, other.id);
    }
}