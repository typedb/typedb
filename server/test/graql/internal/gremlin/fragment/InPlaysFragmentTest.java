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

package grakn.core.graql.internal.gremlin.fragment;

import grakn.core.graql.Graql;
import grakn.core.graql.Var;
import grakn.core.graql.internal.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static grakn.core.graql.internal.Schema.EdgeLabel.PLAYS;
import static grakn.core.graql.internal.Schema.EdgeLabel.SUB;
import static grakn.core.graql.internal.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class InPlaysFragmentTest {

    private final Var start = Graql.var();
    private final Var end = Graql.var();
    private final Fragment fragment = Fragments.inPlays(null, start, end, false);

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsDownwards() {
        GraphTraversal<Vertex, Vertex> traversal = __.V();
        fragment.applyTraversalInner(traversal, null, ImmutableSet.of());

        // Make sure we check this is a vertex, then traverse plays and downwards subs once
        assertThat(traversal, is(__.V()
                .has(Schema.VertexProperty.ID.name())
                .in(PLAYS.getLabel())
                .union(__.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())), __.<Vertex>until(__.loops().is(Fragments.TRAVERSE_ALL_SUB_EDGES)).repeat(__.in(SUB.getLabel())).emit()).unfold()
        ));
    }
}