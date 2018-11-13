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

import grakn.core.graql.Var;
import grakn.core.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.internal.gremlin.spanningtree.graph.Node;
import grakn.core.graql.internal.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.internal.gremlin.spanningtree.util.Weighted;
import grakn.core.server.kb.internal.TransactionImpl;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.Graql.var;
import static grakn.core.graql.internal.Schema.BaseType.RELATIONSHIP_TYPE;
import static grakn.core.graql.internal.Schema.EdgeLabel.ATTRIBUTE;
import static grakn.core.graql.internal.Schema.EdgeLabel.ISA;
import static grakn.core.graql.internal.Schema.EdgeLabel.PLAYS;
import static grakn.core.graql.internal.Schema.EdgeLabel.RELATES;
import static grakn.core.graql.internal.Schema.EdgeLabel.SHARD;
import static grakn.core.graql.internal.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;
import static grakn.core.graql.internal.Schema.VertexProperty.IS_IMPLICIT;
import static grakn.core.graql.internal.Schema.VertexProperty.LABEL_ID;

/**
 * A fragment representing traversing an isa edge from type to instance.
 *
 */

@AutoValue
public abstract class InIsaFragment extends Fragment {

    @Override
    public abstract Var end();

    abstract boolean mayHaveEdgeInstances();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionImpl<?> graph, Collection<Var> vars) {

        GraphTraversal<Vertex, Vertex> vertexTraversal = Fragments.isVertex(traversal);

        if (mayHaveEdgeInstances()) {
            GraphTraversal<Vertex, Vertex> isImplicitRelationType =
                    __.<Vertex>hasLabel(RELATIONSHIP_TYPE.name()).has(IS_IMPLICIT.name(), true);

            GraphTraversal<Vertex, Element> toVertexAndEdgeInstances = Fragments.union(ImmutableSet.of(
                    toVertexInstances(__.identity()),
                    toEdgeInstances()
            ));

            return choose(vertexTraversal, isImplicitRelationType,
                    toVertexAndEdgeInstances,
                    toVertexInstances(__.identity())
            );
        } else {
            return toVertexInstances(vertexTraversal);
        }
    }

    /**
     * A type-safe way to do `a.choose(pred, whenTrue, whenFalse)`, as `choose(a, pred, whenTrue, whenFalse)`.
     * This is because the default signature is too restrictive
     */
    private <S, E1, E2> GraphTraversal<S, E2> choose(
            GraphTraversal<S, E1> traversal, GraphTraversal<E1, ?> traversalPredicate,
            GraphTraversal<E1, ? extends E2> trueChoice, GraphTraversal<E1, ? extends E2> falseChoice) {

        // This is safe. The generics for `GraphTraversal#choose` are more restrictive than necessary
        //noinspection unchecked
        return traversal.choose(
                traversalPredicate, (GraphTraversal<S, E2>) trueChoice, (GraphTraversal<S, E2>) falseChoice);
    }

    private <S> GraphTraversal<S, Vertex> toVertexInstances(GraphTraversal<S, Vertex> traversal) {
        return traversal.in(SHARD.getLabel()).in(ISA.getLabel());
    }

    private GraphTraversal<Vertex, Edge> toEdgeInstances() {
        Var type = var();
        Var labelId = var();

        // There is no fast way to retrieve all edge instances, because edges cannot be globally indexed.
        // This is a best-effort, that uses the schema to limit the search space...

        // First retrieve the type ID
        GraphTraversal<Vertex, Vertex> traversal =
                __.<Vertex>as(type.name()).values(LABEL_ID.name()).as(labelId.name()).select(type.name());

        // Next, navigate the schema to all possible types whose instances can be in this relation
        traversal = Fragments.inSubs(traversal.out(RELATES.getLabel()).in(PLAYS.getLabel()));

        // Navigate to all (vertex) instances of those types
        // (we do not need to navigate to edge instances, because edge instances cannot be role-players)
        traversal = toVertexInstances(traversal);

        // Finally, navigate to all relation edges with the correct type attached to these instances
        return traversal.outE(ATTRIBUTE.getLabel())
                .has(RELATIONSHIP_TYPE_LABEL_ID.name(), __.where(P.eq(labelId.name())));
    }

    @Override
    public String name() {
        return String.format("<-[isa:%s]-", mayHaveEdgeInstances() ? "with-edges" : "");
    }

    @Override
    public double internalFragmentCost() {
        return COST_INSTANCES_PER_TYPE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> directedEdges(Map<NodeId, Node> nodes,
                                                           Map<Node, Map<Node, Fragment>> edges) {
        return directedEdges(NodeId.NodeType.ISA, nodes, edges);
    }
}
