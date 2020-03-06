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
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

import static grakn.core.graql.planning.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract class for the fragments that traverse Schema.EdgeLabel#ROLE_PLAYER edges: InRolePlayerFragment and
 * OutRolePlayerFragment.
 */
abstract class AbstractRolePlayerFragment extends EdgeFragment {

    static final Variable RELATION_EDGE = reservedVar("RELATION_EDGE");
    static final Variable RELATION_DIRECTION = reservedVar("RELATION_DIRECTION");

    AbstractRolePlayerFragment(VarProperty varProperty, Variable start, Variable end) {
        super(varProperty, start, end);
    }

    private static Variable reservedVar(String value) {
        return new Variable(value, Variable.Type.Reserved);
    }

    abstract Variable edge();

    abstract @Nullable
    Variable role();

    abstract @Nullable ImmutableSet<Label> roleLabels();

    abstract @Nullable ImmutableSet<Label> relationTypeLabels();

    final String innerName() {
        Variable role = role();
        String roleString = role != null ? " role:" + role.symbol() : "";
        String rels = displayOptionalTypeLabels("rels", relationTypeLabels());
        String roles = displayOptionalTypeLabels("roles", roleLabels());
        return "[" + Schema.EdgeLabel.ROLE_PLAYER.getLabel() + ":" + edge().symbol() + roleString + rels + roles + "]";
    }

    @Override
    final ImmutableSet<Variable> otherVars() {
        ImmutableSet.Builder<Variable> builder = ImmutableSet.<Variable>builder().add(edge());
        Variable role = role();
        if (role != null) builder.add(role);
        return builder.build();
    }


    @Override
    protected Node startNode() {
        return new InstanceNode(NodeId.of(NodeId.Type.VAR, start()));
    }

    @Override
    protected Node endNode() {
        return new InstanceNode(NodeId.of(NodeId.Type.VAR, end()));
    }

    @Override
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.VAR, edge());
    }


    static void applyLabelsToTraversal(
            GraphTraversal<?, Edge> traversal, Schema.EdgeProperty property,
            @Nullable Set<Label> typeLabels, ConceptManager conceptManager) {

        if (typeLabels != null) {
            Set<Integer> typeIds =
                    typeLabels.stream().map(label -> conceptManager.convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        }
    }

    /**
     * Optionally traverse from a Schema.EdgeLabel#ROLE_PLAYER edge to the Role it mentions, plus any super-types.
     *
     * @param traversal the traversal, starting from the Schema.EdgeLabel#ROLE_PLAYER  edge
     * @param role the variable to assign to the role. If not present, do nothing
     * @param edgeProperty the edge property to look up the role label ID
     */
    static void traverseToRole(
            GraphTraversal<?, Edge> traversal, @Nullable Variable role, Schema.EdgeProperty edgeProperty,
            Collection<Variable> vars) {
        if (role != null) {
            Variable edge = new Variable();
            traversal.as(edge.symbol());
            Fragments.outSubs(Fragments.traverseSchemaConceptFromEdge(traversal, edgeProperty));
            assignVar(traversal, role, vars).select(edge.symbol());
        }
    }
}
