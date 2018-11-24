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

package grakn.core.graql.query.aggregate;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.answer.Value;
import grakn.core.graql.util.PrimitiveNumberComparator;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Aggregate that finds maximum of a {@link Match}.
 */
public class MaxAggregate implements Aggregate<Value> {

    private final Var varName;

    public MaxAggregate(Var varName) {
        this.varName = varName;
    }

    @Override
    public List<Value> apply(Stream<? extends ConceptMap> stream) {
        PrimitiveNumberComparator comparator = new PrimitiveNumberComparator();
        Number number = stream.map(this::getValue).max(comparator).orElse(null);
        if (number == null) return Collections.emptyList();
        else return Collections.singletonList( new Value(number));
    }

    @Override
    public String toString() {
        return "max " + varName;
    }

    private Number getValue(ConceptMap result) {
        Object value = result.get(varName).asAttribute().value();

        if (value instanceof Number) return (Number) value;
        else throw new RuntimeException("Invalid attempt to compare non-Numbers in Max Aggregate function");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MaxAggregate that = (MaxAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
