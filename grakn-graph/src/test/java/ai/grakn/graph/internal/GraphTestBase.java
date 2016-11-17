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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class GraphTestBase {
    protected AbstractGraknGraph graknGraphBatch;
    protected AbstractGraknGraph graknGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUpGraph() {
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraph();
        graknGraphBatch = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraphBatchLoading();
    }

    @After
    public void destroyGraphAccessManager() throws Exception {
        graknGraph.close();
    }
}
