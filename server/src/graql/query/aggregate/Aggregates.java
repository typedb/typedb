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

import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.Var;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;

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
