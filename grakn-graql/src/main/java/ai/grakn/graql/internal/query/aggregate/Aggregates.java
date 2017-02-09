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
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.VarName;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factory for making {@link Aggregate} implementations.
 *
 * @author Felix Chapman
 */
public class Aggregates {

    private Aggregates() {}

    /**
     * Aggregate that finds mean of a match query.
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Double>> mean(VarName varName) {
        return new MeanAggregate(varName);
    }

    /**
     * Aggregate that counts results of a match query.
     */
    public static Aggregate<Object, Long> count() {
        return new CountAggregate();
    }

    /**
     * Aggregate that groups results of a match query by variable name
     * @param varName the variable name to group results by
     */
    public static Aggregate<Map<VarName, Concept>, Map<Concept, List<Map<VarName, Concept>>>> group(VarName varName) {
        return group(varName, list());
    }

    /**
     * Aggregate that groups results of a match query by variable name, applying an aggregate to each group.
     * @param <T> the type of each group
     */
    public static <T> Aggregate<Map<VarName, Concept>, Map<Concept, T>> group(
            VarName varName, Aggregate<? super Map<VarName, Concept>, T> innerAggregate
    ) {
        return new GroupAggregate<>(varName, innerAggregate);
    }

    /**
     * An aggregate that changes match query results into a list.
     * @param <T> the type of the results of the match query
     */
    public static <T> Aggregate<T, List<T>> list() {
        return new ListAggregate<>();
    }

    /**
     * Aggregate that finds maximum of a match query.
     */
    public static <T extends Comparable<T>> Aggregate<Map<VarName, Concept>, Optional<T>> max(VarName varName) {
        return new MaxAggregate<>(varName);
    }

    /**
     * Aggregate that finds median of a match query.
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Number>> median(VarName varName) {
        return new MedianAggregate(varName);
    }

    /**
     * Aggregate that finds the unbiased sample standard deviation of a match query
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Double>> std(VarName varName) {
        return new StdAggregate(varName);
    }

    /**
     * Aggregate that finds minimum of a match query.
     */
    public static <T extends Comparable<T>> Aggregate<Map<VarName, Concept>, Optional<T>> min(VarName varName) {
        return new MinAggregate<>(varName);
    }

    /**
     * An aggregate that combines several aggregates together into a map (where keys are the names of the aggregates)
     * @param <S> the type of the match query results
     * @param <T> the type of the aggregate results
     */
    public static <S, T> Aggregate<S, Map<String, T>> select(
            ImmutableSet<NamedAggregate<? super S, ? extends T>> aggregates
    ) {
        return new SelectAggregate<>(aggregates);
    }

    /**
     * Aggregate that sums results of a match query.
     */
    public static Aggregate<Map<VarName, Concept>, Number> sum(VarName varName) {
        return new SumAggregate(varName);
    }
}
