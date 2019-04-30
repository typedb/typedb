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

package grakn.core.graql.gremlin.fragment;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import grakn.core.concept.ConceptId;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.property.IdProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;

import static grakn.core.concept.printer.StringPrinter.conceptId;

@AutoValue
abstract class IdFragment extends Fragment {

    abstract ConceptId id();

    public Fragment transform(Map<Variable, ConceptId> transform) {
        ConceptId toId = transform.get(start());
        if (toId == null) return this;
        return new AutoValue_IdFragment(new IdProperty(toId.getValue()), start(), toId);
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionOLTP graph, Collection<Variable> vars) {
        if (canOperateOnEdges()) {
            // Handle both edges and vertices
            return traversal.or(
                    edgeTraversal(),
                    vertexTraversal(__.identity())
            );
        } else {
            return vertexTraversal(traversal);
        }
    }

    private GraphTraversal<Vertex, Vertex> vertexTraversal(GraphTraversal<Vertex, ? extends Element> traversal) {
        return (GraphTraversal<Vertex, Vertex>) traversal.hasId(Schema.elementId(id()));
    }

    private GraphTraversal<Edge, Edge> edgeTraversal() {
        return Fragments.union(
                ImmutableSet.of(
                    __.hasId(Schema.elementId(id())),
                    __.has(Schema.VertexProperty.EDGE_RELATION_ID.name(), id().getValue())
                )
        );
    }

    @Override
    public String name() {
        return "[id:" + conceptId(id()) + "]";
    }

    @Override
    public double internalFragmentCost() {
        return COST_NODE_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }

    @Override
    public boolean canOperateOnEdges() {
        return Schema.isEdgeId(id());
    }
}
