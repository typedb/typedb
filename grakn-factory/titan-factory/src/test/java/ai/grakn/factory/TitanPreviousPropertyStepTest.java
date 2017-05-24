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
 *
 */

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Felix Chapman
 */
public class TitanPreviousPropertyStepTest extends TitanTestBase {

    private static GraphTraversalSource graph;

    @BeforeClass
    public static void setupClass() throws InterruptedException {
        graph = titanGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph().traversal();
    }

    @Test
    public void whenFilteringAPropertyToBeEqualToAPreviousProperty_UseTitanGraphStep() {
        GraphTraversal traversal = graph.V().outE().values("foo").as("x").V().has("bar", __.where(P.eq("x")));

        GraphTraversal expected = optimisedTraversal();

        traversal.asAdmin().applyStrategies();

        assertEquals(expected, traversal);
    }

    private GraphTraversal optimisedTraversal() {
        GraphTraversal<Vertex, Object> expected = graph.V().outE().values("foo").as("x");

        GraphTraversal.Admin<Vertex, Object> admin = expected.asAdmin();
        TitanPreviousPropertyStep<Vertex, Vertex> graphStep = new TitanPreviousPropertyStep<>(admin, "bar", "x");
        admin.addStep(graphStep);

        admin.applyStrategies();

        return expected;
    }
}
