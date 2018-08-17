/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.janus;

import ai.grakn.GraknTxType;
import ai.grakn.janus.JanusPreviousPropertyStep;
import ai.grakn.janus.JanusPreviousPropertyStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
@SuppressWarnings("unused") // We create some vertices and edges for the tests but don't reference them
public class JanusPreviousPropertyStepTest extends JanusTestBase {

    private static final GraphTraversalSource tinker = TinkerGraph.open().traversal();

    private static final GraphTraversalSource janus = janusGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph().traversal();

    private static final Vertex vertexWithFoo = janus.addV().property("v prop", "foo").next();
    private static final Vertex vertexWithBar = janus.addV().property("v prop", "bar").next();
    private static final Vertex vertexWithoutProperty = janus.addV().next();
    private static final Edge edge = janus.V(vertexWithoutProperty).as("x").addE("self").to("x").property("e prop", "foo").next();

    @Test
    public void whenFilteringAPropertyToBeEqualToAPreviousProperty_UseJanusGraphStep() {
        GraphTraversal traversal = optimisableTraversal(janus);

        GraphTraversal expected = optimisedTraversal(janus);

        traversal.asAdmin().applyStrategies();

        assertEquals(expected, traversal);
    }

    @Test
    public void whenExecutingOptimisedTraversal_ResultIsCorrect() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(janus);

        List<Vertex> vertices = traversal.toList();

        assertThat(vertices, contains(vertexWithFoo));
    }

    @Test
    public void whenExecutingManuallyOptimisedTraversal_ResultIsCorrect() {
        GraphTraversal<Vertex, Vertex> traversal = optimisedTraversal(janus);

        List<Vertex> vertices = traversal.toList();

        assertThat(vertices, contains(vertexWithFoo));
    }

    @Test
    public void whenUsingAJanusGraph_ApplyStrategy() {
        GraphTraversal<?, ?> traversal = optimisableTraversal(janus);
        traversal.asAdmin().applyStrategies();

        List<Step> steps = traversal.asAdmin().getSteps();
        assertThat(steps, hasItem(instanceOf(JanusPreviousPropertyStep.class)));
    }

    @Test
    public void whenUsingANonJanusGraph_DontApplyStrategy() {
        GraphTraversal<?, ?> traversal = optimisableTraversal(tinker);
        traversal.asAdmin().applyStrategies();

        List<Step> steps = traversal.asAdmin().getSteps();
        assertThat(steps, not(hasItem(instanceOf(JanusPreviousPropertyStep.class))));
    }

    @Test
    public void whenUsingAJanusGraph_TheTitanPreviousPropertyStepStrategyIsInList() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(janus);
        List<TraversalStrategy<?>> strategies = traversal.asAdmin().getStrategies().toList();

        assertThat(strategies, hasItem(instanceOf(JanusPreviousPropertyStepStrategy.class)));
    }

    @Test
    public void whenUsingANonJanusGraph_TheTitanPreviousPropertyStepStrategyIsNotInList() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(tinker);
        List<TraversalStrategy<?>> strategies = traversal.asAdmin().getStrategies().toList();

        assertThat(strategies, not(hasItem(instanceOf(JanusPreviousPropertyStepStrategy.class))));
    }

    private GraphTraversal<Vertex, Vertex> optimisableTraversal(GraphTraversalSource g) {
        return g.V().outE().values("e prop").as("x").V().has("v prop", __.where(P.eq("x")));
    }

    private GraphTraversal<Vertex, Vertex> optimisedTraversal(GraphTraversalSource g) {
        GraphTraversal expected = g.V().outE().values("e prop").as("x");

        GraphTraversal.Admin<Vertex, Object> admin = expected.asAdmin();
        JanusPreviousPropertyStep<?> graphStep = new JanusPreviousPropertyStep<>(admin, "v prop", "x");
        admin.addStep(graphStep);

        admin.applyStrategies();

        return expected;
    }

}
