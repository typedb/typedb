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
import ai.grakn.graql.VarName;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.applyTypeLabelToTraversal;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabel;
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

    private final VarName edge;
    private final Optional<TypeLabel> roleType;
    private final Optional<TypeLabel> relationType;

    InShortcutFragment(
            VarName rolePlayer, VarName edge, VarName relation, Optional<TypeLabel> roleType,
            Optional<TypeLabel> relationType) {
        super(rolePlayer, relation, edge);
        this.edge = edge;
        this.roleType = roleType;
        this.relationType = relationType;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal, GraknGraph graph) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.inE(SHORTCUT.getLabel()).as(edge.getValue());
        applyTypeLabelToTraversal(edgeTraversal, ROLE_TYPE_ID, roleType, graph);
        applyTypeLabelToTraversal(edgeTraversal, RELATION_TYPE_ID, relationType, graph);
        edgeTraversal.outV();
    }

    @Override
    public String getName() {
        String rel = displayOptionalTypeLabel(relationType);
        String role = displayOptionalTypeLabel(roleType);
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
        if (!roleType.equals(that.roleType)) return false;
        return relationType.equals(that.relationType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + roleType.hashCode();
        result = 31 * result + relationType.hashCode();
        return result;
    }
}
