/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Concept;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Match;
import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
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
     * Aggregate that finds mean of a {@link Match}.
     */
    public static Aggregate<Answer, Optional<Double>> mean(Var varName) {
        return new MeanAggregate(varName);
    }

    /**
     * Aggregate that counts results of a {@link Match}.
     */
    public static Aggregate<Object, Long> count() {
        return new CountAggregate();
    }

    /**
     * Aggregate that checks if there are any results
     */
    public static Aggregate<Object,Boolean> ask() {
        return AskAggregate.get();
    }

    /**
     * Aggregate that groups results of a {@link Match} by variable name
     * @param varName the variable name to group results by
     */
    public static Aggregate<Answer, Map<Concept, List<Answer>>> group(Var varName) {
        return group(varName, list());
    }

    /**
     * Aggregate that groups results of a {@link Match} by variable name, applying an aggregate to each group.
     * @param <T> the type of each group
     */
    public static <T> Aggregate<Answer, Map<Concept, T>> group(
            Var varName, Aggregate<? super Answer, T> innerAggregate
    ) {
        return new GroupAggregate<>(varName, innerAggregate);
    }

    /**
     * An aggregate that changes {@link Match} results into a list.
     * @param <T> the type of the results of the {@link Match}
     */
    public static <T> Aggregate<T, List<T>> list() {
        return new ListAggregate<>();
    }

    /**
     * Aggregate that finds maximum of a {@link Match}.
     */
    public static <T extends Comparable<T>> Aggregate<Answer, Optional<T>> max(Var varName) {
        return new MaxAggregate<>(varName);
    }

    /**
     * Aggregate that finds median of a {@link Match}.
     */
    public static Aggregate<Answer, Optional<Number>> median(Var varName) {
        return new MedianAggregate(varName);
    }

    /**
     * Aggregate that finds the unbiased sample standard deviation of a {@link Match}
     */
    public static Aggregate<Answer, Optional<Double>> std(Var varName) {
        return new StdAggregate(varName);
    }

    /**
     * Aggregate that finds minimum of a {@link Match}.
     */
    public static <T extends Comparable<T>> Aggregate<Answer, Optional<T>> min(Var varName) {
        return new MinAggregate<>(varName);
    }

    /**
     * An aggregate that combines several aggregates together into a map (where keys are the names of the aggregates)
     * @param <S> the type of the {@link Match} results
     * @param <T> the type of the aggregate results
     */
    public static <S, T> Aggregate<S, Map<String, T>> select(
            ImmutableSet<NamedAggregate<? super S, ? extends T>> aggregates
    ) {
        return new SelectAggregate<>(aggregates);
    }

    /**
     * Aggregate that sums results of a {@link Match}.
     */
    public static Aggregate<Answer, Number> sum(Var varName) {
        return new SumAggregate(varName);
    }
}
