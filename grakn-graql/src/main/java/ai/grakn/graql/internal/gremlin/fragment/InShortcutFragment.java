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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.RELATION_DIRECTION;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.RELATION_EDGE;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.applyTypeLabelsToTraversal;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.traverseRoleFromShortcutEdge;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_LABEL_ID;

/**
 * A fragment representing traversing a {@link ai.grakn.util.Schema.EdgeLabel#SHORTCUT} edge from the role-player to
 * the relation.
 * <p>
 * Part of a {@link ai.grakn.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link OutShortcutFragment}.
 *
 * @author Felix Chapman
 */
class InShortcutFragment extends Fragment {

    private final Var edge;
    private final Optional<Var> role;
    private final Optional<Set<Label>> roleLabels;
    private final Optional<Set<Label>> relationTypeLabels;

    InShortcutFragment(VarProperty varProperty,
                       Var rolePlayer, Var edge, Var relation, Optional<Var> role, Optional<Set<Label>> roleLabels,
                       Optional<Set<Label>> relationTypeLabels) {
        super(varProperty, rolePlayer, relation, edge, optionalVarToArray(role));
        this.edge = edge;
        this.role = role;
        this.roleLabels = roleLabels;
        this.relationTypeLabels = relationTypeLabels;
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {

        return Fragments.union(Fragments.isVertex(traversal), ImmutableSet.of(
                reifiedRelationTraversal(graph),
                edgeRelationTraversal(graph, Direction.OUT, RELATIONSHIP_ROLE_OWNER_LABEL_ID),
                edgeRelationTraversal(graph, Direction.IN, RELATIONSHIP_ROLE_VALUE_LABEL_ID)
        ));
    }

    private GraphTraversal<Vertex, Vertex> reifiedRelationTraversal(GraknTx graph) {
        GraphTraversal<Vertex, Edge> edgeTraversal = __.<Vertex>inE(SHORTCUT.getLabel()).as(edge.getValue());

        // Filter by any provided type labels
        applyTypeLabelsToTraversal(edgeTraversal, ROLE_LABEL_ID, roleLabels, graph);
        applyTypeLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels, graph);

        traverseRoleFromShortcutEdge(edgeTraversal, role, ROLE_LABEL_ID);

        return edgeTraversal.outV();
    }

    private GraphTraversal<Vertex, Edge> edgeRelationTraversal(
            GraknTx graph, Direction direction, Schema.EdgeProperty roleProperty) {

        GraphTraversal<Vertex, Edge> edgeTraversal = __.toE(direction, Schema.EdgeLabel.RESOURCE.getLabel());

        // Identify the relation - role-player pair by combining the relation edge and direction into a map
        edgeTraversal.as(RELATION_EDGE).constant(direction).as(RELATION_DIRECTION);
        edgeTraversal.select(Pop.last, RELATION_EDGE, RELATION_DIRECTION).as(edge.getValue()).select(RELATION_EDGE);

        // Filter by any provided type labels
        applyTypeLabelsToTraversal(edgeTraversal, roleProperty, roleLabels, graph);
        applyTypeLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels, graph);

        traverseRoleFromShortcutEdge(edgeTraversal, role, roleProperty);

        return edgeTraversal;
    }

    @Override
    public String getName() {
        String role = this.role.map(rt -> " role:" + rt.shortName()).orElse("");
        String rels = displayOptionalTypeLabels("rels", relationTypeLabels);
        String roles = displayOptionalTypeLabels("roles", roleLabels);
        return "<-[shortcut:" + edge.shortName() + role + rels + roles + "]-";
    }

    @Override
    public double fragmentCost() {
        return COST_RELATIONS_PER_INSTANCE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return getDirectedEdges(edge, nodes, edges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InShortcutFragment that = (InShortcutFragment) o;

        if (!edge.equals(that.edge)) return false;
        if (!roleLabels.equals(that.roleLabels)) return false;
        return relationTypeLabels.equals(that.relationTypeLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + roleLabels.hashCode();
        result = 31 * result + relationTypeLabels.hashCode();
        return result;
    }
}
