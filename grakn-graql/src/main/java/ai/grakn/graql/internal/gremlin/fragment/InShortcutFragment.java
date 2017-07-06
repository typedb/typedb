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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.applyTypeLabelsToTraversal;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.traverseRoleTypeFromShortcutEdge;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_ID;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_TYPE_ID;

/**
 * A fragment representing traversing a {@link ai.grakn.util.Schema.EdgeLabel#SHORTCUT} edge from the role-player to
 * the relation.
 * <p>
 * Part of a {@link ai.grakn.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link OutShortcutFragment}.
 *
 * @author Felix Chapman
 */
class InShortcutFragment extends AbstractFragment {

    private final Var edge;
    private final Optional<Var> roleType;
    private final Optional<Set<Label>> roleTypeLabels;
    private final Optional<Set<Label>> relationTypeLabels;

    InShortcutFragment(VarProperty varProperty,
                       Var rolePlayer, Var edge, Var relation, Optional<Var> roleType, Optional<Set<Label>> roleTypeLabels,
                       Optional<Set<Label>> relationTypeLabels) {
        super(varProperty, rolePlayer, relation, edge, optionalVarToArray(roleType));
        this.edge = edge;
        this.roleType = roleType;
        this.roleTypeLabels = roleTypeLabels;
        this.relationTypeLabels = relationTypeLabels;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal, GraknGraph graph) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.inE(SHORTCUT.getLabel()).as(edge.getValue());

        // Filter by any provided type labels
        applyTypeLabelsToTraversal(edgeTraversal, ROLE_TYPE_ID, roleTypeLabels, graph);
        applyTypeLabelsToTraversal(edgeTraversal, RELATION_TYPE_ID, relationTypeLabels, graph);

        traverseRoleTypeFromShortcutEdge(edgeTraversal, roleType);

        edgeTraversal.outV();
    }

    @Override
    public String getName() {
        String role = roleType.map(rt -> " role:" + rt.shortName()).orElse("");
        String rels = displayOptionalTypeLabels("rels", relationTypeLabels);
        String roles = displayOptionalTypeLabels("roles", roleTypeLabels);
        return "<-[shortcut:" + edge.shortName() + role + rels + roles + "]-";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return COST_RELATIONS_PER_INSTANCE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<String, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return getDirectedEdgesBoth(edge, nodes, edges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InShortcutFragment that = (InShortcutFragment) o;

        if (!edge.equals(that.edge)) return false;
        if (!roleTypeLabels.equals(that.roleTypeLabels)) return false;
        return relationTypeLabels.equals(that.relationTypeLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + roleTypeLabels.hashCode();
        result = 31 * result + relationTypeLabels.hashCode();
        return result;
    }
}
