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
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class GraphTestBase {
    protected GraknSession graknSession;
    protected AbstractGraknGraph<?> graknGraph;
    private AbstractGraknGraph<?> graknGraphBatch;
    private String keyspace = UUID.randomUUID().toString().replaceAll("-", "a");

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUpGraph() {
        graknSession = Grakn.session(Grakn.IN_MEMORY, keyspace);
        graknGraph = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
    }

    @After
    public void closeSession() throws Exception {
        graknGraph.close();
        if(graknGraphBatch != null) graknGraphBatch.close();
        graknSession.close();
    }

    protected AbstractGraknGraph<?> switchToBatchGraph(){
        graknGraph.close();
        graknGraphBatch = (AbstractGraknGraph) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.BATCH);
        return graknGraphBatch;
    }
}
