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

package ai.grakn.factory;
import ai.grakn.GraknComputer;
import ai.grakn.util.REST;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 *
 */
public class GraknSessionMock extends GraknSessionImpl {
    private final String keyspace;
    private final String uri;

    public GraknSessionMock(String keyspace, String uri) {
        super(keyspace.toLowerCase(), uri);
        this.keyspace = keyspace;
        this.uri = uri;
    }

    public GraknComputer getGraphComputer(int numberOfWorkers) {
        ConfiguredFactory configuredFactory = configureGraphFactory(keyspace, uri, REST.GraphConfig.COMPUTER);
        Graph graph = configuredFactory.factory.getTinkerPopGraph(false);
        return new GraknComputerMock(graph, configuredFactory.graphComputer, numberOfWorkers);
    }
}
