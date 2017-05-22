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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.applyTypeLabelsToTraversal;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
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
    private final Optional<Set<TypeLabel>> roleTypes;
    private final Optional<Set<TypeLabel>> relationTypes;

    InShortcutFragment(
            Var rolePlayer, Var edge, Var relation, Optional<Set<TypeLabel>> roleTypes,
            Optional<Set<TypeLabel>> relationTypes) {
        super(rolePlayer, relation, edge);
        this.edge = edge;
        this.roleTypes = roleTypes;
        this.relationTypes = relationTypes;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal, GraknGraph graph) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.inE(SHORTCUT.getLabel()).as(edge.getValue());
        applyTypeLabelsToTraversal(edgeTraversal, ROLE_TYPE_ID, roleTypes, graph);
        applyTypeLabelsToTraversal(edgeTraversal, RELATION_TYPE_ID, relationTypes, graph);
        edgeTraversal.outV();
    }

    @Override
    public String getName() {
        String rel = displayOptionalTypeLabels(relationTypes);
        String role = displayOptionalTypeLabels(roleTypes);
        return "<-[shortcut:" + edge.shortName() + rel + role + "]-";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_RELATIONS_PER_INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InShortcutFragment that = (InShortcutFragment) o;

        if (!edge.equals(that.edge)) return false;
        if (!roleTypes.equals(that.roleTypes)) return false;
        return relationTypes.equals(that.relationTypes);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + roleTypes.hashCode();
        result = 31 * result + relationTypes.hashCode();
        return result;
    }
}
