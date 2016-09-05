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

import io.mindmaps.core.model.Concept;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Aggregate that sums results of a match query.
 */
public class SumAggregate extends AbstractAggregate<Map<String, Concept>, Number> {

    private final String varName;

    public SumAggregate(String varName) {
        this.varName = varName;
    }

    @Override
    public Number apply(Stream<? extends Map<String, Concept>> stream) {
        return stream.map(result -> (Number) result.get(varName).asResource().getValue()).reduce(0, this::add);
    }

    private Number add(Number x, Number y) {
        // This method is necessary because Number doesn't support '+' because java!
        if (x instanceof Long || y instanceof Long) {
            return x.longValue() + y.longValue();
        } else {
            return x.doubleValue() + y.doubleValue();
        }
    }

    @Override
    public String toString() {
        return "sum $" + varName;
    }
}
