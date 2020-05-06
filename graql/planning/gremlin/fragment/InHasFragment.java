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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class InHasFragment extends EdgeFragment {
    private final ImmutableSet<Label> attributeTypeLabels;
    private Variable edgeVariable;
    // for backwards compatibility
    private final Set<Label> implicitRelationTypes;
    private final Set<Label> ownerRoles;
    private final Set<Label> valueRoles;

    public InHasFragment(VarProperty varProperty, Variable owner, Variable attribute, ImmutableSet<Label> attributeTypeLabels) {
        super(varProperty, attribute, owner);
        this.attributeTypeLabels = attributeTypeLabels;
        edgeVariable = new Variable();



        implicitRelationTypes = attributeTypeLabels.stream().map(label -> Schema.ImplicitType.HAS.getLabel(label)).collect(Collectors.toSet());
        attributeTypeLabels.forEach(label -> implicitRelationTypes.add(Schema.ImplicitType.KEY.getLabel(label)));

        ownerRoles = new HashSet<>();
        attributeTypeLabels.forEach(label -> {
            ownerRoles.add(Schema.ImplicitType.HAS_OWNER.getLabel(label));
            ownerRoles.add(Schema.ImplicitType.KEY_OWNER.getLabel(label));
        });

        valueRoles = new HashSet<>();
        attributeTypeLabels.forEach(label -> {
            valueRoles.add(Schema.ImplicitType.HAS_VALUE.getLabel(label));
            valueRoles.add(Schema.ImplicitType.KEY_VALUE.getLabel(label));
        });
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

        //        rolePlayer(property, property.relation().var(), edge1, var, null,
//                ImmutableSet.of(hasOwnerRole, keyOwnerRole), ImmutableSet.of(has, key)),
//                //value rolePlayer edge
//                rolePlayer(property, property.relation().var(), edge2, property.attribute().var(), null,
//                        ImmutableSet.of(hasValueRole, keyValueRole), ImmutableSet.of(has, key)),
//                neq(property, edge1, edge2)


        // extend the traversal with a UNION of 2 paths:
        // (start) ATTR <-[value]- rel node -[owner]-> OWNER, owner != value edge
        // (start) ATTR <-[edge] - OWNER
        return null;
    }

    @Override
    public String name() {
        return "<-[has-xxx]-";
    }

    @Override
    public double internalFragmentCost() {
        // TODO - use COST_OWNERS_PER_ATTRIBUTE;
        return 0.0;
    }
}
