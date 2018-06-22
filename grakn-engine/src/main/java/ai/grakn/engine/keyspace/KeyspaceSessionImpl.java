/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.keyspace;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.KeyspaceSession;
import ai.grakn.factory.GraknTxFactoryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;

/**
 * Provides transactions to access "graknsystem" which is Grakn system keyspace - used by KeyspaceStore to keep track
 * of available keyspaces
 *
 * @author Marco Scoppetta
 */

public class KeyspaceSessionImpl implements KeyspaceSession {

    //TODO: centralise this
    private final static Keyspace SYSTEM_KB_KEYSPACE = Keyspace.of("graknsystem");
    private final EmbeddedGraknSession session;

    public KeyspaceSessionImpl(GraknConfig config) {
        session = EmbeddedGraknSession.createEngineSession(SYSTEM_KB_KEYSPACE, engineURI(config), config, GraknTxFactoryBuilder.getInstance());
    }

    @Override
    public EmbeddedGraknTx tx(GraknTxType txType) {
        return session.open(txType);
    }

    private String engineURI(GraknConfig config) {
        return config.getProperty(GraknConfigKey.SERVER_HOST_NAME) + ":" + config.getProperty(GraknConfigKey.SERVER_PORT);
    }
}
