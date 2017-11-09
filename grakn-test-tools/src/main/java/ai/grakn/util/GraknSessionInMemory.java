/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.util;

import ai.grakn.GraknComputer;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.factory.FactoryBuilder;
import ai.grakn.kb.internal.computer.GraknComputerImpl;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Properties;

/**
 * An in memory {@link GraknSession} which is used by {@link SampleKBLoader} to load test KBs
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class GraknSessionInMemory implements GraknSession {
    private final String uri;
    private final Keyspace keyspace;
    private final Properties config;

    public GraknSessionInMemory(Keyspace keyspace, String uri, Properties config) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.config = config;
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        throw new UnsupportedOperationException("In memory grakn session does not support this function");
    }

    @Override
    public GraknComputer getGraphComputer() {
        Graph graph = FactoryBuilder.getFactory(this, true).getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraknTxOperationException {
        throw new UnsupportedOperationException("In memory grakn session does not support this function");
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    @Override
    public Properties config() {
        return config;
    }
}
