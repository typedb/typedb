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
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.BaseType.RELATION_TYPE;
import java.util.Map;
import java.util.Set;

import static ai.grakn.util.Schema.EdgeLabel.ISA;
import static ai.grakn.util.Schema.EdgeLabel.PLAYS;
import static ai.grakn.util.Schema.EdgeLabel.RELATES;
import static ai.grakn.util.Schema.EdgeLabel.RESOURCE;
import static ai.grakn.util.Schema.EdgeLabel.SHARD;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_LABEL_ID;
import static ai.grakn.util.Schema.VertexProperty.IS_IMPLICIT;
import static ai.grakn.util.Schema.VertexProperty.LABEL_ID;

class InIsaFragment extends AbstractFragment {

    InIsaFragment(VarProperty varProperty, Var start, Var end) {
        super(varProperty, start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<? extends Element, ? extends Element> traversal, GraknGraph graph) {
        Fragments.inSubs((GraphTraversal<Vertex, Vertex>) traversal);

        GraphTraversal<Vertex, Vertex> isImplicitRelationType =
                __.<Vertex>hasLabel(RELATION_TYPE.name()).has(IS_IMPLICIT.name(), true);

        traversal.choose(isImplicitRelationType,
                __.union(
                        toVertexInstances(__.identity()),
                        (GraphTraversal) toEdgeInstances()
                ),
                toVertexInstances(__.identity())
        );
    }

    private <T> GraphTraversal<T, Vertex> toVertexInstances(GraphTraversal<T, Vertex> traversal) {
        return traversal.in(SHARD.getLabel()).in(ISA.getLabel());
    }

    private GraphTraversal<?, Edge> toEdgeInstances() {
        Var type = var();
        Var labelId = var();

        // There is no fast way to retrieve all edge instances, because edges cannot be globally indexed.
        // This is a best-effort, that uses the ontology to limit the search space...

        // First retrieve the type ID
        GraphTraversal<Vertex, Vertex> traversal =
                __.<Vertex>as(type.getValue()).values(LABEL_ID.name()).as(labelId.getValue()).select(type.getValue());

        // Next, navigate the ontology to all possible types whose instances can be in this relation
        traversal = Fragments.inSubs(traversal.out(RELATES.getLabel()).in(PLAYS.getLabel()));

        // Navigate to all (vertex) instances of those types
        // (we do not need to navigate to edge instances, because edge instances cannot be role-players)
        traversal = toVertexInstances(traversal);

        // Finally, navigate to all relation edges with the correct type attached to these instances
        return traversal.outE(RESOURCE.getLabel())
                .has(RELATION_TYPE_LABEL_ID.name(), __.where(P.eq(labelId.getValue())));
    }

    @Override
    public String getName() {
        return "<-[isa]-";
    }

    @Override
    public double fragmentCost() {
        return COST_INSTANCES_PER_TYPE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return getDirectedEdges(NodeId.NodeType.ISA, nodes, edges);
    }
}
