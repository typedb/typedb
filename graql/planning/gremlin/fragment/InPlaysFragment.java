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
import java.util.Objects;

import static grakn.core.core.Schema.EdgeLabel.PLAYS;

class InPlaysFragment extends EdgeFragment {
    private final boolean required;

    InPlaysFragment(
            @Nullable VarProperty varProperty,
            Variable start,
            Variable end,
            boolean required) {
        super(varProperty, start, end);
        this.required = required;
    }

    private boolean required() {
        return required;
    }


    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {
        GraphTraversal<Vertex, Vertex> vertexTraversal = Fragments.isVertex(traversal);
        if (required()) {
            vertexTraversal.inE(PLAYS.getLabel()).has(Schema.EdgeProperty.REQUIRED.name()).otherV();
        } else {
            vertexTraversal.in(PLAYS.getLabel());
        }

        return Fragments.inSubs(vertexTraversal);
    }

    @Override
    public String name() {
        if (required()) {
            return "<-[plays:required]-";
        } else {
            return "<-[plays]-";
        }
    }

    @Override
    public double internalFragmentCost() {
        return COST_TYPES_PER_ROLE;
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
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.PLAYS, new HashSet<>(Arrays.asList(start(), end())));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof InPlaysFragment) {
            InPlaysFragment that = (InPlaysFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.end.equals(that.end()))
                    && (this.required == that.required());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, start, end, required);
    }
}
