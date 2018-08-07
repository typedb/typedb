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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.Var;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

import static ai.grakn.graql.internal.pattern.Patterns.RELATION_DIRECTION;
import static ai.grakn.graql.internal.pattern.Patterns.RELATION_EDGE;
import static ai.grakn.util.Schema.EdgeLabel.ROLE_PLAYER;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_LABEL_ID;

/**
 * A fragment representing traversing a {@link ai.grakn.util.Schema.EdgeLabel#ROLE_PLAYER} edge from the role-player to
 * the relation.
 * <p>
 * Part of a {@link ai.grakn.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link OutRolePlayerFragment}.
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class InRolePlayerFragment extends AbstractRolePlayerFragment {

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, EmbeddedGraknTx<?> tx, Collection<Var> vars) {

        return Fragments.union(Fragments.isVertex(traversal), ImmutableSet.of(
                reifiedRelationTraversal(tx, vars),
                edgeRelationTraversal(tx, Direction.OUT, RELATIONSHIP_ROLE_OWNER_LABEL_ID, vars),
                edgeRelationTraversal(tx, Direction.IN, RELATIONSHIP_ROLE_VALUE_LABEL_ID, vars)
        ));
    }

    private GraphTraversal<Vertex, Vertex> reifiedRelationTraversal(EmbeddedGraknTx<?> tx, Collection<Var> vars) {
        GraphTraversal<Vertex, Edge> edgeTraversal = __.<Vertex>inE(ROLE_PLAYER.getLabel()).as(edge().name());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, ROLE_LABEL_ID, roleLabels(), tx);
        applyLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels(), tx);

        traverseToRole(edgeTraversal, role(), ROLE_LABEL_ID, vars);

        return edgeTraversal.outV();
    }

    private GraphTraversal<Vertex, Edge> edgeRelationTraversal(
            EmbeddedGraknTx<?> tx, Direction direction, Schema.EdgeProperty roleProperty, Collection<Var> vars) {

        GraphTraversal<Vertex, Edge> edgeTraversal = __.toE(direction, Schema.EdgeLabel.ATTRIBUTE.getLabel());

        // Identify the relation - role-player pair by combining the relationship edge and direction into a map
        edgeTraversal.as(RELATION_EDGE.name()).constant(direction).as(RELATION_DIRECTION.name());
        edgeTraversal.select(Pop.last, RELATION_EDGE.name(), RELATION_DIRECTION.name()).as(edge().name()).select(RELATION_EDGE.name());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, roleProperty, roleLabels(), tx);
        applyLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels(), tx);

        traverseToRole(edgeTraversal, role(), roleProperty, vars);

        return edgeTraversal;
    }

    @Override
    public String name() {
        return "<-" + innerName() + "-";
    }

    @Override
    public double internalFragmentCost() {
        return COST_RELATIONS_PER_INSTANCE;
    }
}
