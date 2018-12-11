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
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.answer.Value;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds median of a match clause.
 */
public class MedianAggregate implements Aggregate<Value> {

    private final Variable varName;

    public MedianAggregate(Variable varName) {
        this.varName = varName;
    }

    @Override
    public List<Value> apply(Stream<? extends ConceptMap> stream) {
        List<Number> results = stream
                .map(result -> ((Number) result.get(varName).asAttribute().value()))
                .sorted()
                .collect(toList());

        int size = results.size();
        int halveFloor = Math.floorDiv(size - 1, 2);
        int halveCeiling = (int) Math.ceil((size - 1) / 2.0);

        if (size == 0) {
            return Collections.emptyList();
        } else if (size % 2 == 1) {
            // Take exact middle result
            return Collections.singletonList(new Value(results.get(halveFloor)));
        } else {
            // Take average of middle results
            Number result = (results.get(halveFloor).doubleValue() + results.get(halveCeiling).doubleValue()) / 2;
            return Collections.singletonList(new Value(result));
        }
    }

    @Override
    public String toString() {
        return "median " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MedianAggregate that = (MedianAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
