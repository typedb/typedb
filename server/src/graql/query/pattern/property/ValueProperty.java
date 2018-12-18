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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.predicate.ValuePredicate;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Represents the {@code value} property on an attribute.
 * This property can be queried or inserted.
 * This property matches only resources whose value matches the given a value predicate.
 */
public class ValueProperty extends VarProperty {

    public static final String NAME = "";
    private final ValuePredicate predicate;

    public ValueProperty(ValuePredicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Null predicate");
        }
        this.predicate = predicate;
    }

    public ValuePredicate predicate() {
        return predicate;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return predicate().toString();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public String toString() {
        return getProperty();
    }

    @Override
    public Stream<Statement> innerStatements() {
        return CommonUtil.optionalToStream(predicate().getInnerVar());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.value(this, start, predicate()));
    }

    @Override
    public Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Object value = predicate().equalsValue().orElseThrow(GraqlQueryException::insertPredicate);
            executor.builder(var).value(value);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ValueProperty) {
            ValueProperty that = (ValueProperty) o;
            return (this.predicate.equals(that.predicate()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.predicate.hashCode();
        return h;
    }
}
