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
import ai.grakn.GraknTxType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class GraphTestBase {
    private AbstractGraknGraph<?> graknGraphBatch;
    protected AbstractGraknGraph<?> graknGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUpGraph() {
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        graknGraphBatch = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.BATCH);
        graknGraphBatch.close();
        graknGraph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
    }

    @After
    public void destroyGraphAccessManager() throws Exception {
        graknGraph.close();
    }

    public AbstractGraknGraph<?> switchToBatchGraph(){
        graknGraph.close();
        graknGraphBatch = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, graknGraphBatch.getKeyspace()).open(GraknTxType.BATCH);
        return graknGraphBatch;
    }
}
