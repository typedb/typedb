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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.applyTypeLabelsToTraversal;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.traverseRoleFromShortcutEdge;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_ID;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_TYPE_ID;

/**
 * A fragment representing traversing a {@link ai.grakn.util.Schema.EdgeLabel#SHORTCUT} edge from the relation to the
 * role-player.
 * <p>
 * Part of a {@link ai.grakn.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link InShortcutFragment}.
 *
 * @author Felix Chapman
 */
class OutShortcutFragment extends AbstractFragment {

    private final Var edge;

    private final Optional<Var> role;
    private final Optional<Set<Label>> roleLabels;
    private final Optional<Set<Label>> relationTypeLabels;

    OutShortcutFragment(VarProperty varProperty,
            Var relation, Var edge, Var rolePlayer, Optional<Var> role, Optional<Set<Label>> roleLabels,
            Optional<Set<Label>> relationTypeLabels) {
            super(varProperty, relation, rolePlayer, edge, optionalVarToArray(role));
            this.edge = edge;
            this.role = role;
            this.roleLabels = roleLabels;
            this.relationTypeLabels = relationTypeLabels;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal, GraknGraph graph) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.outE(SHORTCUT.getLabel()).as(edge.getValue());

        // Filter by any provided type labels
        applyTypeLabelsToTraversal(edgeTraversal, ROLE_TYPE_ID, roleLabels, graph);
        applyTypeLabelsToTraversal(edgeTraversal, RELATION_TYPE_ID, relationTypeLabels, graph);

        traverseRoleFromShortcutEdge(edgeTraversal, role);

        edgeTraversal.inV();
    }

    @Override
    public String getName() {
        String role = this.role.map(rt -> " role:" + rt.shortName()).orElse("");
        String rels = displayOptionalTypeLabels("rels", relationTypeLabels);
        String roles = displayOptionalTypeLabels("roles", roleLabels);
        return "-[shortcut:" + edge.shortName() + role + rels + roles + "]->";
    }

    @Override
    public double fragmentCost(double previousCost) {
        long numRolePlayers = roleLabels.isPresent() ? NUM_ROLE_PLAYERS_PER_ROLE : NUM_ROLE_PLAYERS_PER_RELATION;
        return previousCost * numRolePlayers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        OutShortcutFragment that = (OutShortcutFragment) o;

        if (!edge.equals(that.edge)) return false;
        if (!role.equals(that.role)) return false;
        if (!roleLabels.equals(that.roleLabels)) return false;
        return relationTypeLabels.equals(that.relationTypeLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + role.hashCode();
        result = 31 * result + roleLabels.hashCode();
        result = 31 * result + relationTypeLabels.hashCode();
        return result;
    }
}
