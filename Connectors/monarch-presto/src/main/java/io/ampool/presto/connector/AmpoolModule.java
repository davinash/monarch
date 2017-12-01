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

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;

import static java.util.Objects.requireNonNull;

public class AmpoolModule implements Module
{
    private static final Logger log = Logger.get(AmpoolModule.class);

    private final String connectorId;
    private final AmpoolClient ampoolClient;
    private final TypeManager typeManager;

    public AmpoolModule(String connectorId, AmpoolClient ampoolClient, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.ampoolClient = requireNonNull(ampoolClient, "ampoolClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        log.info("INFORMATION: AmpoolModule created successfully.");
    }

    @Override
    public void configure(Binder binder)
    {
        log.info("INFORMATION: AmpoolMetadata configure() called.");

        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(AmpoolConnectorID.class).toInstance(new AmpoolConnectorID(connectorId));
        binder.bind(AmpoolConnector.class).in(Scopes.SINGLETON);
        binder.bind(AmpoolMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AmpoolClient.class).in(Scopes.SINGLETON);
        binder.bind(AmpoolSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AmpoolRecordSetProvider.class).in(Scopes.SINGLETON);
        //configBinder(binder).bindConfig(AmpoolConfig.class);

        /*
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(AmpoolTable.class));
        */
    }

    /*
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
    */
}
