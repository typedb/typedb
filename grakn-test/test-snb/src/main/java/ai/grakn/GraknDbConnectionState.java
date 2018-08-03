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
package ai.grakn;

import ai.grakn.engine.GraknConfig;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.GraknTxFactoryBuilder;
import com.ldbc.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the database connection for grakn. Essentially a proxy for a session.
 *
 * @author sheldon, felix
 */
public class GraknDbConnectionState extends DbConnectionState {

    private final GraknSession session;

    /**
     * Initiate the grakn session.
     *
     * @param properties the properties from the ldbc properties file
     */
    public GraknDbConnectionState(Map<String, String> properties) {

        String keyspace;

        keyspace = properties.get("ai.grakn.keyspace");

        session = EmbeddedGraknSession.createEngineSession(Keyspace.of(keyspace), GraknConfig.create(), GraknTxFactoryBuilder.getInstance());
    }

    @Override
    public void close(){
        session.close();
    }

    /**
     * Get the open grakn session.
     *
     * @return the open session
     */
    GraknSession session() {
        return this.session;
    }
}
