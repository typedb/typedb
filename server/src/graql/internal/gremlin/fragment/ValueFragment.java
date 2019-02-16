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

import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.internal.executor.property.ValueExecutor.Operation.Comparison;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class ValueFragment extends Fragment {

    private final VarProperty varProperty;
    private final Variable start;
    private final ValueExecutor.Operation<?, ?> operation;

    ValueFragment(@Nullable VarProperty varProperty, Variable start, ValueExecutor.Operation<?, ?> operation) {
        this.varProperty = varProperty;
        if (start == null) {
            throw new NullPointerException("Null start");
        }
        this.start = start;
        if (operation == null) {
            throw new NullPointerException("Null operation");
        }
        this.operation = operation;
    }

    private ValueExecutor.Operation<?, ?> predicate() {
        return operation;
    }

    @Nullable
    @Override
    public VarProperty varProperty() {
        return varProperty;
    }

    @Override
    public Variable start() {
        return start;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionOLTP graph, Collection<Variable> vars
    ) {

        return predicate().apply(traversal);
    }

    @Override
    public String name() {
        return "[value:" + predicate() + "]";
    }

    @Override
    public double internalFragmentCost() {
        if (predicate().isValueEquality()) {
            return COST_NODE_INDEX_VALUE;
        } else {
            // Assume approximately half of values will satisfy a filter
            return COST_NODE_UNSPECIFIC_PREDICATE;
        }
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return predicate().isValueEquality() && dependencies().isEmpty();
    }

    @Override
    public Set<Variable> dependencies() {
        if (operation instanceof Comparison.Variable) {
            return Collections.singleton(((Comparison.Variable) operation).value().var());
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueFragment that = (ValueFragment) o;

        return (Objects.equals(this.varProperty, that.varProperty) &&
                this.start.equals(that.start()) &&
                this.operation.equals(that.predicate()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.start.hashCode();
        h *= 1000003;
        h ^= this.operation.hashCode();
        return h;
    }
}
