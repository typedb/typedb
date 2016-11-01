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

package io.mindmaps.graql.internal.query.predicate;

import io.mindmaps.graql.ValuePredicate;

public class Predicates {

    private Predicates() {}

    public static ValuePredicate regex(String pattern) {
        return new RegexPredicate(pattern);
    }

    public static ValuePredicate neq(Object value) {
        return new NeqPredicate(value);
    }

    public static ValuePredicate lt(Object value) {
        return new LtPredicate(value);
    }

    public static ValuePredicate lte(Object value) {
        return new LtePredicate(value);
    }

    public static ValuePredicate gt(Object value) {
        return new GtPredicate(value);
    }

    public static ValuePredicate gte(Object value) {
        return new GtePredicate(value);
    }

    public static ValuePredicate eq(Object value) {
        return new EqPredicate(value);
    }

    public static ValuePredicate contains(String substring) {
        return new ContainsPredicate(substring);
    }
}
