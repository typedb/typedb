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

package ai.grakn.factory;

import ai.grakn.GraknSystemProperty;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.util.GraknTestUtil;

import java.io.File;
import java.nio.file.Paths;

/**
 * <p>
 *     Test {@link ai.grakn.GraknSession}
 * </p>
 *
 * <p>
 *     A {@link ai.grakn.GraknSession} used for testing which bypasses the need to hit the REST controller responsible
 *     for feeding in the appropriate config file.
 *
 *     This session can be used to provide {@link ai.grakn.GraknTx}'s dependent on either a TinkerGraph or
 *     JanusGraph
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class GraknSessionLocal extends EmbeddedGraknSession {

    private GraknSessionLocal(Keyspace keyspace, GraknConfig config) {
        super(keyspace, config,  GraknTxFactoryBuilder.getInstance());
    }

    public static GraknSessionLocal create(Keyspace keyspace) {
        return new GraknSessionLocal(keyspace, null);
    }

    public static GraknSessionLocal create(Keyspace keyspace, GraknConfig config) {
        return new GraknSessionLocal(keyspace, config);
    }


}
