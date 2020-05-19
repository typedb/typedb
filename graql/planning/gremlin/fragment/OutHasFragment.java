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

import grakn.core.core.Schema;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.graph.SchemaNode;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * Fragment for traversing from a type, to the attribute types it can own
 * We traverse to attributes that are owned as both normal HAS or as KEY
 */
public class OutHasFragment extends EdgeFragment {

    public OutHasFragment(@Nullable VarProperty varProperty, Variable start, Variable end) {
        super(varProperty, start, end);
    }

    @Override
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.HAS, new HashSet<>(Arrays.asList(start(), end())));
    }

    @Override
    protected Node startNode() {
        return new SchemaNode(NodeId.of(NodeId.Type.VAR, start()));
    }

    @Override
    protected Node endNode() {
        return new SchemaNode(NodeId.of(NodeId.Type.VAR, end()));
    }

    @Override
    GraphTraversal<Vertex, Vertex> applyTraversalInner(GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {
        // a type can own any attribute that any of its parents (self inclusive) can own
        return Fragments
                .outSubs(Fragments.isVertex(traversal))
                .out(Schema.EdgeLabel.HAS.getLabel(), Schema.EdgeLabel.KEY.getLabel());
    }

    @Override
    public String name() {
        return "-[has]->";
    }

    @Override
    public double internalFragmentCost() {
        return COST_ROLES_PER_TYPE;
    }
}