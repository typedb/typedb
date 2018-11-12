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

package grakn.core.graql.internal.query.predicate;

import grakn.core.graql.ValuePredicate;
import grakn.core.graql.admin.VarPatternAdmin;

/**
 * Factory method for {@link ValuePredicate} implementations.
 *
 */
public class Predicates {

    private Predicates() {}

    public static ValuePredicate regex(String pattern) {
        return RegexPredicate.of(pattern);
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

    public static ValuePredicate contains(VarPatternAdmin var) {
        return new ContainsPredicate(var);
    }
}
