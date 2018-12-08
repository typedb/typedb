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

import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Role;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.internal.gremlin.spanningtree.graph.Node;
import grakn.core.graql.internal.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.internal.gremlin.spanningtree.util.Weighted;
import grakn.core.server.session.TransactionImpl;
import grakn.core.graql.internal.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract class for the fragments that traverse {@link Schema.EdgeLabel#ROLE_PLAYER} edges: {@link InRolePlayerFragment} and
 * {@link OutRolePlayerFragment}.
 *
 */
public abstract class AbstractRolePlayerFragment extends Fragment {

    static final Variable RELATION_EDGE = reservedVar("RELATION_EDGE");
    static final Variable RELATION_DIRECTION = reservedVar("RELATION_DIRECTION");

    private static Variable reservedVar(String value) {
        return new Variable(value, Variable.Type.Reserved);
    }

    @Override
    public abstract Variable end();

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
    public final Set<Weighted<DirectedEdge<Node>>> directedEdges(
            Map<NodeId, Node> nodes, Map<Node, Map<Node, Fragment>> edges) {
        return directedEdges(edge(), nodes, edges);
    }

    static void applyLabelsToTraversal(
            GraphTraversal<?, Edge> traversal, Schema.EdgeProperty property,
            @Nullable Set<Label> typeLabels, TransactionImpl<?> tx) {

        if (typeLabels != null) {
            Set<Integer> typeIds =
                    typeLabels.stream().map(label -> tx.convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        }
    }

    /**
     * Optionally traverse from a {@link Schema.EdgeLabel#ROLE_PLAYER} edge to the {@link Role} it mentions, plus any super-types.
     *
     * @param traversal the traversal, starting from the {@link Schema.EdgeLabel#ROLE_PLAYER}  edge
     * @param role the variable to assign to the role. If not present, do nothing
     * @param edgeProperty the edge property to look up the role label ID
     */
    static void traverseToRole(
            GraphTraversal<?, Edge> traversal, @Nullable Variable role, Schema.EdgeProperty edgeProperty,
            Collection<Variable> vars) {
        if (role != null) {
            Variable edge = Pattern.var();
            traversal.as(edge.symbol());
            Fragments.outSubs(Fragments.traverseSchemaConceptFromEdge(traversal, edgeProperty));
            assignVar(traversal, role, vars).select(edge.symbol());
        }
    }
}
