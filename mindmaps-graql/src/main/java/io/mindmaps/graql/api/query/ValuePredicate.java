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

package io.mindmaps.graql.api.query;

import io.mindmaps.graql.internal.StringConverter;
import io.mindmaps.graql.internal.query.ValuePredicateImpl;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * a predicate on a value in a query.
 * <p>
 * A predicate describes a predicate (true/false) function that can be tested against some value in the graph.
 * <p>
 * Predicates can be combined together using the methods {@code and}, {@code or}, {@code any} and {@code all}.
 */
public interface ValuePredicate {

    /**
     * @param value the value
     * @return a predicate that is true when a value equals the specified value
     */
    static ValuePredicate eq(Object value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.eq(value), StringConverter.valueToString(value), value, true);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value does not equal the specified value
     */
    static ValuePredicate neq(Object value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.neq(value), "!= " + StringConverter.valueToString(value), value, false);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly greater than the specified value
     */
    static ValuePredicate gt(Comparable value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.gt(value), "> " + StringConverter.valueToString(value), value, false);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is greater or equal to the specified value
     */
    static ValuePredicate gte(Comparable value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.gte(value), ">= " + StringConverter.valueToString(value), value, false);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly less than the specified value
     */
    static ValuePredicate lt(Comparable value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.lt(value), "< " + StringConverter.valueToString(value), value, false);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is less or equal to the specified value
     */
    static ValuePredicate lte(Comparable value) {
        Objects.requireNonNull(value);
        return new ValuePredicateImpl(P.lte(value), "<= " + StringConverter.valueToString(value), value, false);
    }

    /**
     * @param predicates an array of predicates
     * @return a predicate that returns true when all the predicates are true
     */
    static ValuePredicate all(ValuePredicate... predicates) {
        return Arrays.asList(predicates).stream().reduce(ValuePredicate::and).get();
    }

    /**
     * @param predicates an array of predicates
     * @return a predicate that returns true when any of the predicates are true
     */
    static ValuePredicate any(ValuePredicate... predicates) {
        return Arrays.asList(predicates).stream().reduce(ValuePredicate::or).get();
    }

    /**
     * @param pattern a regex pattern
     * @return a predicate that returns true when a value matches the given regular expression
     */
    static ValuePredicate regex(String pattern) {
        Objects.requireNonNull(pattern);
        return new ValuePredicateImpl(
                new P<>((value, p) -> java.util.regex.Pattern.matches((String) p, (String) value), pattern),
                "/" + pattern + "/",
                pattern,
                false
        );
    }

    /**
     * @param substring a substring to match
     * @return a predicate that returns true when a value contains the given substring
     */
    static ValuePredicate contains(String substring) {
        Objects.requireNonNull(substring);
        return new ValuePredicateImpl(
                new P<>((value, s) -> ((String) value).contains((String) s), substring),
                "contains " + StringConverter.valueToString(substring),
                substring,
                false
        );
    }

    /**
     * @param other the other predicate
     * @return a predicate that returns true when both predicates are true
     */
    ValuePredicate and(ValuePredicate other);

    /**
     * @param other the other predicate
     * @return a predicate that returns true when either predicate is true
     */
    ValuePredicate or(ValuePredicate other);

    /**
     * @return an Admin class allowing inspection of this predicate
     */
    Admin admin();

    /**
     * Admin class for inspecting a ValuePredicate
     */
    interface Admin extends ValuePredicate {
        /**
         * @return whether this predicate is specific (e.g. "eq" is specific, "regex" is not)
         */
        boolean isSpecific();

        /**
         * @return the value comparing against, if this is an "equality" predicate, otherwise nothing
         */
        Optional<Object> equalsValue();

        /**
         * @return all values referred to in the predicate (including within 'ors' and 'ands')
         */
        Set<Object> getInnerValues();

        /**
         * @return the gremlin predicate object this ValuePredicate wraps
         */
        P<Object> getPredicate();
    }
}
