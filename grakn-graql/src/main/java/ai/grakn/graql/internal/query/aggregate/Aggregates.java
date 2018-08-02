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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.AnswerGroup;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.Value;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Factory for making {@link Aggregate} implementations.
 */
public class Aggregates {

    private Aggregates() {}

    /**
     * Aggregate that counts results of a {@link Match}.
     */
    public static Aggregate<Value> count(Var... vars) {
        return new CountAggregate(new HashSet<>(Arrays.asList(vars)));
    }

    public static Aggregate<Value> count(Set<Var> vars) {
        return new CountAggregate(vars);
    }

    /**
     * Aggregate that sums results of a {@link Match}.
     */
    public static Aggregate<Value> sum(Var varName) {
        return new SumAggregate(varName);
    }

    /**
     * Aggregate that finds minimum of a {@link Match}.
     */
    public static Aggregate<Value> min(Var varName) {
        return new MinAggregate(varName);
    }

    /**
     * Aggregate that finds maximum of a {@link Match}.
     */
    public static Aggregate<Value> max(Var varName) {
        return new MaxAggregate(varName);
    }

    /**
     * Aggregate that finds mean of a {@link Match}.
     */
    public static Aggregate<Value> mean(Var varName) {
        return new MeanAggregate(varName);
    }

    /**
     * Aggregate that finds median of a {@link Match}.
     */
    public static Aggregate<Value> median(Var varName) {
        return new MedianAggregate(varName);
    }

    /**
     * Aggregate that finds the unbiased sample standard deviation of a {@link Match}
     */
    public static Aggregate<Value> std(Var varName) {
        return new StdAggregate(varName);
    }

    /**
     * An aggregate that changes {@link Match} results into a list.
     */
    public static Aggregate<ConceptMap> list() {
        return new ListAggregate();
    }

    /**
     * Aggregate that groups results of a {@link Match} by variable name
     * @param varName the variable name to group results by
     */
    public static Aggregate<AnswerGroup<ConceptMap>> group(Var varName) {
        return group(varName, new ListAggregate());
    }

    /**
     * Aggregate that groups results of a {@link Match} by variable name, applying an aggregate to each group.
     * @param <T> the type of each group
     */
    public static <T extends Answer> Aggregate<AnswerGroup<T>> group(Var varName, Aggregate<T> innerAggregate) {
        return new GroupAggregate<>(varName, innerAggregate);
    }
}
