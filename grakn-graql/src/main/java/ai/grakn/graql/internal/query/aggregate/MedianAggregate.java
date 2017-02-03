/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds median of a match query.
 */
class MedianAggregate extends AbstractAggregate<Map<VarName, Concept>, Optional<Number>> {

    private final VarName varName;

    MedianAggregate(VarName varName) {
        this.varName = varName;
    }

    @Override
    public Optional<Number> apply(Stream<? extends Map<VarName, Concept>> stream) {
        List<Number> results = stream
                .map(result -> ((Number) result.get(varName).asResource().getValue()))
                .sorted()
                .collect(toList());

        int size = results.size();
        int halveFloor = Math.floorDiv(size - 1, 2);
        int halveCeiling = (int) Math.ceil((size - 1) / 2.0);

        if (size == 0) {
            return Optional.empty();
        } else if (size % 2 == 1) {
            // Take exact middle result
            return Optional.of(results.get(halveFloor));
        } else {
            // Take average of middle results
            return Optional.of((results.get(halveFloor).doubleValue() + results.get(halveCeiling).doubleValue()) / 2);
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
