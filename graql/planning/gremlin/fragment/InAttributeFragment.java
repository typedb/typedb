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
 *
 */

package grakn.core.graql.planning.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import grakn.core.core.Schema;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

/**
 * Traverse from an attribute to the owners over the IN edge pointing at the attribute
 * TODO we may in the future be able to filter the edges by a set of types the owner is known to have
 */
public class InAttributeFragment extends EdgeFragment {
    private Variable edgeVariable;
    // for backwards compatibility

    public InAttributeFragment(VarProperty varProperty, Variable attribute, Variable owner, Variable edge) {
        super(varProperty, attribute, owner);
        edgeVariable = edge;
    }

    @Override
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.ATTRIBUTE, edgeVariable);
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
    GraphTraversal<Vertex, ? extends Element> applyTraversalInner(GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {
        // (start) ATTR <-[edge] - OWNER
        return Fragments.union(
                Fragments.isVertex(traversal),
                ImmutableSet.of(attributeToOwners())
        );
    }

    private GraphTraversal<Vertex, Vertex> attributeToOwners() {
        GraphTraversal<Vertex, Edge> edgeTraversal = __.inE(Schema.EdgeLabel.ATTRIBUTE.getLabel());
        return edgeTraversal.outV();
    }

    @Override
    public String name() {
        return "<-[has-xxx]-";
    }

    @Override
    public double internalFragmentCost() {
        // TODO - use COST_OWNERS_PER_ATTRIBUTE;
        return COST_RELATIONS_PER_INSTANCE;
    }
}
