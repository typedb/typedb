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

import grakn.core.graql.query.pattern.Var;
import grakn.core.server.session.TransactionImpl;
import grakn.core.graql.internal.Schema;
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

import static grakn.core.graql.internal.Schema.EdgeLabel.ROLE_PLAYER;
import static grakn.core.graql.internal.Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID;
import static grakn.core.graql.internal.Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID;
import static grakn.core.graql.internal.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;
import static grakn.core.graql.internal.Schema.EdgeProperty.ROLE_LABEL_ID;

/**
 * A fragment representing traversing a {@link Schema.EdgeLabel#ROLE_PLAYER} edge from the role-player to
 * the relation.
 * <p>
 * Part of a {@link grakn.core.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link OutRolePlayerFragment}.
 *
 */
@AutoValue
abstract class InRolePlayerFragment extends AbstractRolePlayerFragment {

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionImpl<?> tx, Collection<Var> vars) {

        return Fragments.union(Fragments.isVertex(traversal), ImmutableSet.of(
                reifiedRelationTraversal(tx, vars),
                edgeRelationTraversal(tx, Direction.OUT, RELATIONSHIP_ROLE_OWNER_LABEL_ID, vars),
                edgeRelationTraversal(tx, Direction.IN, RELATIONSHIP_ROLE_VALUE_LABEL_ID, vars)
        ));
    }

    private GraphTraversal<Vertex, Vertex> reifiedRelationTraversal(TransactionImpl<?> tx, Collection<Var> vars) {
        GraphTraversal<Vertex, Edge> edgeTraversal = __.inE(ROLE_PLAYER.getLabel()).as(edge().label());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, ROLE_LABEL_ID, roleLabels(), tx);
        applyLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels(), tx);

        traverseToRole(edgeTraversal, role(), ROLE_LABEL_ID, vars);

        return edgeTraversal.outV();
    }

    private GraphTraversal<Vertex, Edge> edgeRelationTraversal(
            TransactionImpl<?> tx, Direction direction, Schema.EdgeProperty roleProperty, Collection<Var> vars) {

        GraphTraversal<Vertex, Edge> edgeTraversal = __.toE(direction, Schema.EdgeLabel.ATTRIBUTE.getLabel());

        // Identify the relation - role-player pair by combining the relationship edge and direction into a map
        edgeTraversal.as(RELATION_EDGE.label()).constant(direction).as(RELATION_DIRECTION.label());
        edgeTraversal.select(Pop.last, RELATION_EDGE.label(), RELATION_DIRECTION.label()).as(edge().label()).select(RELATION_EDGE.label());

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
