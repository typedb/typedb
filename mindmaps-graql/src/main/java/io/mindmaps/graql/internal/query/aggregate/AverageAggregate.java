/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query.aggregate;

import io.mindmaps.graql.Aggregate;
import io.mindmaps.concept.Concept;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds average (mean) of a match query.
 */
class AverageAggregate extends AbstractAggregate<Map<String, Concept>, Optional<Double>> {

    private final String varName;
    private final CountAggregate countAggregate;
    private final Aggregate<Map<String, Concept>, Number> sumAggregate;

    AverageAggregate(String varName) {
        this.varName = varName;
        countAggregate = new CountAggregate();
        sumAggregate = Aggregates.sum(varName);
    }

    @Override
    public Optional<Double> apply(Stream<? extends Map<String, Concept>> stream) {
        List<? extends Map<String, Concept>> list = stream.collect(toList());

        long count = countAggregate.apply(list.stream());

        if (count == 0) {
            return Optional.empty();
        } else {
            Number sum = sumAggregate.apply(list.stream());
            return Optional.of(sum.doubleValue() / count);
        }
    }

    @Override
    public String toString() {
        return "average $" + varName;
    }
}
