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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.core.Schema.EdgeLabel.ROLE_PLAYER;
import static grakn.core.core.Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.RELATION_TYPE_LABEL_ID;
import static grakn.core.core.Schema.EdgeProperty.ROLE_LABEL_ID;
import static java.util.stream.Collectors.toSet;

public class OutHasFragment extends EdgeFragment {
    private final ImmutableSet<Label> attributeTypeLabels;
    private Variable edgeVariable;

    public OutHasFragment(VarProperty varProperty, Variable owner, Variable attribute, Variable edge, ImmutableSet<Label> attributeTypeLabels) {
        super(varProperty, owner, attribute);
        this.attributeTypeLabels = attributeTypeLabels;
        edgeVariable = edge;
    }

    @Override
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.VAR, edgeVariable);
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
        // collect the allowed subtypes
        Set<Label> implicitRelationTypes = new HashSet<>();
        Set<Label> ownerRoles = new HashSet<>();
        Set<Label> valueRoles = new HashSet<>();

        // retrieve matching attribute vertices or any of its subtypes' instances
        attributeTypeLabels.stream()
                .flatMap(attributeTypeLabel -> conceptManager.getAttributeType(attributeTypeLabel.getValue()).subs())
                .forEach(attrType -> {
                    Label attrTypeLabel = attrType.label();
                    implicitRelationTypes.add(Schema.ImplicitType.HAS.getLabel(attrTypeLabel));
                    implicitRelationTypes.add(Schema.ImplicitType.KEY.getLabel(attrTypeLabel));
                    ownerRoles.add(Schema.ImplicitType.HAS_OWNER.getLabel(attrTypeLabel));
                    ownerRoles.add(Schema.ImplicitType.KEY_OWNER.getLabel(attrTypeLabel));
                    valueRoles.add(Schema.ImplicitType.HAS_VALUE.getLabel(attrTypeLabel));
                    valueRoles.add(Schema.ImplicitType.KEY_VALUE.getLabel(attrTypeLabel));
                });


        // extend the traversal with a UNION of 2 paths:
        // (end) ATTR <-[value]- rel node -[owner]-> OWNER (START), owner != value edge
        // (end) ATTR <-[edge]- OWNER (START)
        return Fragments.union(Fragments.isVertex(traversal),
                ImmutableSet.of(
//                        reifiedRelationTraversal(conceptManager, vars, implicitRelationTypes, ownerRoles, valueRoles),
                        edgeRelationTraversal(conceptManager, vars, implicitRelationTypes, ownerRoles, valueRoles))
        );
    }

    private GraphTraversal<Vertex, Vertex> reifiedRelationTraversal(ConceptManager conceptManager, Collection<Variable> vars, Set<Label> implicitRelationTypes, Set<Label> ownerRoles, Set<Label> valueRoles) {
        Variable edge1 = new Variable();
        Variable edge2 = new Variable();
        GraphTraversal<Vertex, Edge> ownerEdge = __.inE(ROLE_PLAYER.getLabel()).as(edge1.symbol());

        // Filter by any provided type labels
        applyLabelsToTraversal(ownerEdge, ROLE_LABEL_ID, ownerRoles, conceptManager);
        applyLabelsToTraversal(ownerEdge, RELATION_TYPE_LABEL_ID, implicitRelationTypes, conceptManager);

        // reach the implicit attribute vertex
        GraphTraversal<Vertex, Vertex> implicitRelationVertex = ownerEdge.outV();

        // traverse outgoing role player edge
        GraphTraversal<Vertex, Edge> valueEdge = implicitRelationVertex.outE(ROLE_PLAYER.getLabel()).as(edge2.symbol());
        // as long as it's different from the one we traversed over already
        valueEdge.where(P.neq(edge1.symbol()));
        applyLabelsToTraversal(valueEdge, ROLE_LABEL_ID, valueRoles, conceptManager);
        applyLabelsToTraversal(valueEdge, RELATION_TYPE_LABEL_ID, implicitRelationTypes, conceptManager);

        GraphTraversal<Vertex, Vertex> attributeVertex = valueEdge.inV();
        return attributeVertex;
    }

    private GraphTraversal<Vertex, Vertex> edgeRelationTraversal(
            ConceptManager conceptManager, Collection<Variable> vars, Set<Label> implicitRelationTypes, Set<Label> ownerRoles, Set<Label> valueRoles) {

        GraphTraversal<Vertex, Edge> edgeTraversal = __.outE(Schema.EdgeLabel.ATTRIBUTE.getLabel());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, RELATION_ROLE_OWNER_LABEL_ID, ownerRoles, conceptManager);
        applyLabelsToTraversal(edgeTraversal, RELATION_ROLE_VALUE_LABEL_ID, valueRoles, conceptManager);
        applyLabelsToTraversal(edgeTraversal, RELATION_TYPE_LABEL_ID, implicitRelationTypes, conceptManager);

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
        return 0.0;
    }
}
