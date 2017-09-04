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
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.displayOptionalTypeLabels;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract class for the fragments that traverse shortcut edges: {@link InShortcutFragment} and
 * {@link OutShortcutFragment}.
 *
 * @author Felix Chapman
 */
public abstract class AbstractShortcutFragment extends Fragment {

    @Override
    public abstract Var end();

    abstract Var edge();

    abstract @Nullable Var role();

    abstract @Nullable ImmutableSet<Label> roleLabels();

    abstract @Nullable ImmutableSet<Label> relationTypeLabels();

    final String innerName() {
        Var role = role();
        String roleString = role != null ? " role:" + role.shortName() : "";
        String rels = displayOptionalTypeLabels("rels", relationTypeLabels());
        String roles = displayOptionalTypeLabels("roles", roleLabels());
        return "[shortcut:" + edge().shortName() + roleString + rels + roles + "]";
    }

    @Override
    final ImmutableSet<Var> otherVars() {
        ImmutableSet.Builder<Var> builder = ImmutableSet.<Var>builder().add(edge());
        Var role = role();
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
            @Nullable Set<Label> typeLabels, GraknTx tx) {

        if (typeLabels != null) {
            Set<Integer> typeIds =
                    typeLabels.stream().map(label -> tx.admin().convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        }
    }

    /**
     * Optionally traverse from a shortcut edge to the role-type it mentions, plus any super-types.
     *
     * @param traversal the traversal, starting from the shortcut edge
     * @param role the variable to assign to the role. If not present, do nothing
     * @param edgeProperty the edge property to look up the role label ID
     */
    static void traverseToRole(
            GraphTraversal<?, Edge> traversal, @Nullable Var role, Schema.EdgeProperty edgeProperty) {
        if (role != null) {
            Var edge = Graql.var();
            traversal.as(edge.getValue());
            Fragments.outSubs(Fragments.traverseSchemaConceptFromEdge(traversal, edgeProperty));
            traversal.as(role.getValue()).select(edge.getValue());
        }
    }
}
