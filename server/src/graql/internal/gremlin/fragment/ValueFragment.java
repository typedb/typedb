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

import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static grakn.core.common.util.CommonUtil.optionalToStream;
import static java.util.stream.Collectors.toSet;

public class ValueFragment extends Fragment {

    private final VarProperty varProperty;
    private final Variable start;
    private final ValuePredicate predicate;

    ValueFragment(@Nullable VarProperty varProperty, Variable start, ValuePredicate predicate) {
        this.varProperty = varProperty;
        if (start == null) {
            throw new NullPointerException("Null start");
        }
        this.start = start;
        if (predicate == null) {
            throw new NullPointerException("Null predicate");
        }
        this.predicate = predicate;
    }

    ValuePredicate predicate() {
        return predicate;
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

        return predicate().applyPredicate(traversal);
    }

    @Override
    public String name() {
        return "[value:" + predicate() + "]";
    }

    @Override
    public double internalFragmentCost() {
        if (predicate().isSpecific()) {
            return COST_NODE_INDEX_VALUE;
        } else {
            // Assume approximately half of values will satisfy a filter
            return COST_NODE_UNSPECIFIC_PREDICATE;
        }
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return predicate().isSpecific() && dependencies().isEmpty();
    }

    @Override
    public Set<Variable> dependencies() {
        return optionalToStream(predicate().getInnerVar()).map(statement -> statement.var()).collect(toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueFragment that = (ValueFragment) o;

        return (Objects.equals(this.varProperty, that.varProperty) &&
                this.start.equals(that.start()) &&
                this.predicate.equals(that.predicate()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.start.hashCode();
        h *= 1000003;
        h ^= this.predicate.hashCode();
        return h;
    }
}
