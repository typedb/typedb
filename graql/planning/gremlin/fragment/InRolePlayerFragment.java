/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.planning.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.manager.ConceptManager;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

import static grakn.core.core.Schema.EdgeLabel.ROLE_PLAYER;
import static grakn.core.core.Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.RELATION_TYPE_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.ROLE_LABEL_ID;

/**
 * A fragment representing traversing a Schema.EdgeLabel#ROLE_PLAYER edge from the role-player to
 * the relation.
 * <p>
 * Part of a EquivalentFragmentSet, along with OutRolePlayerFragment.
 *
 */
class InRolePlayerFragment extends AbstractRolePlayerFragment {

    private final Variable edge;
    private final Variable role;
    private final ImmutableSet<Label> roleLabels;
    private final ImmutableSet<Label> relationTypeLabels;

    InRolePlayerFragment(
            @Nullable VarProperty varProperty,
            Variable start,
            Variable end,
            Variable edge,
            @Nullable Variable role,
            @Nullable ImmutableSet<Label> roleLabels,
            @Nullable ImmutableSet<Label> relationTypeLabels) {
        super(varProperty, start, end);

        this.edge = edge;
        this.role = role;
        this.roleLabels = roleLabels;
        this.relationTypeLabels = relationTypeLabels;
    }

    public Variable end() {
        return end;
    }

    Variable edge() {
        return edge;
    }

    @Nullable
    Variable role() {
        return role;
    }

    @Nullable
    ImmutableSet<Label> roleLabels() {
        return roleLabels;
    }

    @Nullable
    ImmutableSet<Label> relationTypeLabels() {
        return relationTypeLabels;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {

        return Fragments.union(Fragments.isVertex(traversal), ImmutableSet.of(
                reifiedRelationTraversal(conceptManager, vars),
                edgeRelationTraversal(conceptManager, Direction.OUT, RELATION_ROLE_OWNER_LABEL_ID, vars),
                edgeRelationTraversal(conceptManager, Direction.IN, RELATION_ROLE_VALUE_LABEL_ID, vars)
        ));
    }

    private GraphTraversal<Vertex, Vertex> reifiedRelationTraversal(ConceptManager conceptManager, Collection<Variable> vars) {
        GraphTraversal<Vertex, Edge> edgeTraversal = __.inE(ROLE_PLAYER.getLabel()).as(edge().symbol());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, ROLE_LABEL_ID, roleLabels(), conceptManager);
        applyLabelsToTraversal(edgeTraversal, RELATION_TYPE_LABEL_ID, relationTypeLabels(), conceptManager);

        traverseToRole(edgeTraversal, role(), ROLE_LABEL_ID, vars);

        return edgeTraversal.outV();
    }

    private GraphTraversal<Vertex, Edge> edgeRelationTraversal(
            ConceptManager conceptManager, Direction direction, Schema.EdgeProperty roleProperty, Collection<Variable> vars) {

        GraphTraversal<Vertex, Edge> edgeTraversal = __.toE(direction, Schema.EdgeLabel.ATTRIBUTE.getLabel());

        // Identify the relation - role-player pair by combining the relation edge and direction into a map
        edgeTraversal.as(RELATION_EDGE.symbol()).constant(direction).as(RELATION_DIRECTION.symbol());
        edgeTraversal.select(Pop.last, RELATION_EDGE.symbol(), RELATION_DIRECTION.symbol()).as(edge().symbol()).select(RELATION_EDGE.symbol());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, roleProperty, roleLabels(), conceptManager);
        applyLabelsToTraversal(edgeTraversal, RELATION_TYPE_LABEL_ID, relationTypeLabels(), conceptManager);

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

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof InRolePlayerFragment) {
            InRolePlayerFragment that = (InRolePlayerFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.end.equals(that.end()))
                    && (this.edge.equals(that.edge()))
                    && ((this.role == null) ? (that.role() == null) : this.role.equals(that.role()))
                    && ((this.roleLabels == null) ? (that.roleLabels() == null) : this.roleLabels.equals(that.roleLabels()))
                    && ((this.relationTypeLabels == null) ? (that.relationTypeLabels() == null) : this.relationTypeLabels.equals(that.relationTypeLabels()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, start, end, edge, role, roleLabels,relationTypeLabels);
    }
}
