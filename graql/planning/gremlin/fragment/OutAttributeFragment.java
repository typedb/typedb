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
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Set;

import static grakn.core.core.Schema.EdgeProperty.ATTRIBUTE_OWNED_LABEL_ID;
import static java.util.stream.Collectors.toSet;

/**
 * Traverse from an owner instance to attribute instances of the given type labels
 */
public class OutAttributeFragment extends EdgeFragment {
    private final ImmutableSet<Label> attributeTypeLabels;
    private Variable edgeVariable;

    public OutAttributeFragment(VarProperty varProperty, Variable owner, Variable attribute, Variable edge, ImmutableSet<Label> attributeTypeLabels) {
        super(varProperty, owner, attribute);
        this.attributeTypeLabels = attributeTypeLabels;
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
    GraphTraversal<Vertex, Vertex> applyTraversalInner(GraphTraversal<Vertex, Vertex> traversal, ConceptManager conceptManager, Collection<Variable> vars) {
        // (start) ATTR <-[edge]- OWNER (START)
        return edgeRelationTraversal(traversal, conceptManager);
    }

    private GraphTraversal<Vertex, Vertex> edgeRelationTraversal(GraphTraversal<Vertex, Vertex> traversal, ConceptManager conceptManager) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.outE(Schema.EdgeLabel.ATTRIBUTE.getLabel());

        Set<Label> labelsWithSubtypes = attributeTypeLabels
                .stream()
                .flatMap(label -> conceptManager.getSchemaConcept(label).subs())
                .map(type -> type.label())
                .collect(toSet());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, ATTRIBUTE_OWNED_LABEL_ID, labelsWithSubtypes, conceptManager);

        return edgeTraversal.inV();
    }

    private void applyLabelsToTraversal(GraphTraversal<?, Edge> traversal, Schema.EdgeProperty property,
                                        Set<Label> typeLabels, ConceptManager conceptManager) {
        Set<Integer> typeIds =
                typeLabels.stream().map(label -> conceptManager.convertToId(label).getValue()).collect(toSet());
        traversal.has(property.name(), P.within(typeIds));
    }

    @Override
    public String name() {
        return "-[has-xxx]->";
    }

    @Override
    public double internalFragmentCost() {
        // TODO - use COST_OWNERS_PER_ATTRIBUTE;
        return COST_ROLE_PLAYERS_PER_ROLE;
    }
}
