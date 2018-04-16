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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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
    private final static File JANUS_CONFIG_FILE = Paths.get(GraknSystemProperty.PROJECT_RELATIVE_DIR.value() + "/conf/test/janus/grakn.properties").toFile();

    private GraknSessionLocal(Keyspace keyspace, String engineUri, GraknConfig config) {
        super(keyspace, engineUri, config, false, GraknTxFactoryBuilder.getInstance());
    }

    public static GraknSessionLocal create(Keyspace keyspace) {
        return new GraknSessionLocal(keyspace, "fake-local-engine-uri", null);
    }

    public static GraknSessionLocal create(Keyspace keyspace, String engineUri, GraknConfig config) {
        return new GraknSessionLocal(keyspace, engineUri, config);
    }

    @Override
    protected void submitLogs(){
        //No Op
    }

    @Override
    GraknConfig getTxConfig() {
        if (GraknTestUtil.usingJanus()) {
            return getTxJanusConfig();
        } else {
            return getTxInMemoryConfig();
        }
    }

    /**
     * Gets the Janus Properties directly by reading them from disk.
     * This is the part which bypasses the need for a REST endpoint.
     *
     * @return the properties needed to build a JanusGraph
     */
    private GraknConfig getTxJanusConfig() {
        return GraknConfig.read(JANUS_CONFIG_FILE);
    }
}
