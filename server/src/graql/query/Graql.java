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

package grakn.core.graql.query;

import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;
import grakn.core.graql.parser.Parser;
import grakn.core.graql.query.aggregate.CountAggregate;
import grakn.core.graql.query.aggregate.GroupAggregate;
import grakn.core.graql.query.aggregate.ListAggregate;
import grakn.core.graql.query.aggregate.MaxAggregate;
import grakn.core.graql.query.aggregate.MeanAggregate;
import grakn.core.graql.query.aggregate.MedianAggregate;
import grakn.core.graql.query.aggregate.MinAggregate;
import grakn.core.graql.query.aggregate.StdAggregate;
import grakn.core.graql.query.aggregate.SumAggregate;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.predicate.Predicates;
import grakn.core.graql.query.predicate.ValuePredicate;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.query.ComputeQuery.Method;
import static java.util.stream.Collectors.toSet;

/**
 * Main class containing static methods for creating Graql queries.
 * It is recommended you statically import these methods.
 */
public class Graql {

    private static final Parser parser = new Parser();

    public static Parser parser() {
        return parser;
    }

    @CheckReturnValue
    public static <T extends Query<?>> T parse(String queryString) {
        return parser.parseQueryEOF(queryString);
    }

    public static <T extends Query<?>> Stream<T> parseList(String queryString) {
        return parser.parseQueryList(queryString);
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match clause that will find matches of the given patterns
     */
    @CheckReturnValue
    public static MatchClause match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match clause that will find matches of the given patterns
     */
    @CheckReturnValue
    public static MatchClause match(Collection<? extends Pattern> patterns) {
        return new MatchClause(Pattern.and(Collections.unmodifiableSet(new HashSet<>(patterns))));
    }

    /**
     * @param statements an array of variable patterns to insert into the graph
     * @return an insert query that will insert the given variable patterns into the graph
     */
    @CheckReturnValue
    public static InsertQuery insert(Statement... statements) {
        return insert(Arrays.asList(statements));
    }

    /**
     * @param statements a collection of variable patterns to insert into the graph
     * @return an insert query that will insert the given variable patterns into the graph
     */
    @CheckReturnValue
    public static InsertQuery insert(Collection<? extends Statement> statements) {
        return new InsertQuery(null, Collections.unmodifiableList(new ArrayList<>(statements)));
    }

    /**
     * @param statements an array of of statements to define the schema
     * @return a define query that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static DefineQuery define(Statement... statements) {
        return define(Arrays.asList(statements));
    }

    /**
     * @param statements a collection of statements to define the schema
     * @return a define query that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static DefineQuery define(Collection<? extends Statement> statements) {
        return new DefineQuery(Collections.unmodifiableList(new ArrayList<>(statements)));
    }

    /**
     * @param statements an array of statements to undefine the schema
     * @return an undefine query that will remove the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static UndefineQuery undefine(Statement... statements) {
        return undefine(Arrays.asList(statements));
    }

    /**
     * @param statements a collection of statements to undefine the schema
     * @return an undefine query that will remove the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static UndefineQuery undefine(Collection<? extends Statement> statements) {
        return new UndefineQuery(Collections.unmodifiableList(new ArrayList<>(statements)));
    }

    @CheckReturnValue
    public static <T extends Answer> ComputeQuery<T> compute(Method<T> method) {
        return new ComputeQuery<>(method);
    }


    // AGGREGATES

    /**
     * Create an aggregate that will count the results of a query.
     */
    @CheckReturnValue
    public static Aggregate<Value> count(String... vars) {
        return new CountAggregate(Arrays.stream(vars).map(Pattern::var).collect(toSet()));
    }

    /**
     * Aggregate that counts results of a match clause
     */
    @CheckReturnValue
    public static Aggregate<Value> count(Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(vars.length + 1);
        varSet.add(var);
        varSet.addAll(Arrays.asList(vars));
        return new CountAggregate(varSet);
    }

    @CheckReturnValue
    public static Aggregate<Value> count(Collection<Variable> variables) {
        return new CountAggregate(new HashSet<>(variables));
    }

    /**
     * Create an aggregate that will sum the values of a variable.
     */
    @CheckReturnValue
    public static Aggregate<Value> sum(String var) {
        return new SumAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will sum the values of a variable.
     */
    @CheckReturnValue
    public static Aggregate<Value> sum(Variable var) {
        return new SumAggregate(var);
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     *
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> min(String var) {
        return new MinAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     *
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> min(Variable var) {
        return new MinAggregate(var);
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     *
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> max(String var) {
        return new MaxAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     *
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> max(Variable var) {
        return new MaxAggregate(var);
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     *
     * @param var the variable to find the mean of
     */
    @CheckReturnValue
    public static Aggregate<Value> mean(String var) {
        return new MeanAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     *
     * @param var the variable to find the mean of
     */
    @CheckReturnValue
    public static Aggregate<Value> mean(Variable var) {
        return new MeanAggregate(var);
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     *
     * @param var the variable to find the median of
     */
    @CheckReturnValue
    public static Aggregate<Value> median(String var) {
        return new MedianAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     *
     * @param var the variable to find the median of
     */
    @CheckReturnValue
    public static Aggregate<Value> median(Variable var) {
        return new MedianAggregate(var);
    }

    /**
     * Create an aggregate that will find the unbiased sample standard deviation of a variable's values.
     *
     * @param var the variable to find the standard deviation of
     */
    @CheckReturnValue
    public static Aggregate<Value> std(String var) {
        return new StdAggregate(Pattern.var(var));
    }

    /**
     * Create an aggregate that will find the unbiased sample standard deviation of a variable's values.
     *
     * @param var the variable to find the standard deviation of
     */
    @CheckReturnValue
    public static Aggregate<Value> std(Variable var) {
        return new StdAggregate(var);
    }

    /**
     * Create an aggregate that will group a query by a variable.
     *
     * @param var the variable to group results by
     */
    @CheckReturnValue
    public static Aggregate<AnswerGroup<ConceptMap>> group(String var) {
        return group(var, new ListAggregate());
    }

    /**
     * Aggregate that groups results of a match clause by variable name
     *
     * @param varName the variable name to group results by
     */
    public static Aggregate<AnswerGroup<ConceptMap>> group(Variable varName) {
        return group(varName, new ListAggregate());
    }

    /**
     * Create an aggregate that will group a query by a variable and apply the given aggregate to each group
     *
     * @param var       the variable to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T>       the type the aggregate returns
     */
    @CheckReturnValue
    public static <T extends Answer> Aggregate<AnswerGroup<T>> group(String var, Aggregate<T> aggregate) {
        return group(Pattern.var(var), aggregate);
    }

    /**
     * Aggregate that groups results of a match clause by variable name, applying an aggregate to each group.
     *
     * @param <T> the type of each group
     */
    public static <T extends Answer> Aggregate<AnswerGroup<T>> group(Variable varName, Aggregate<T> innerAggregate) {
        return new GroupAggregate<>(varName, innerAggregate);
    }


    // PREDICATES

    /**
     * @param value the value
     * @return a predicate that is true when a value equals the specified value
     */
    @CheckReturnValue
    public static ValuePredicate eq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.eq(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value equals the specified value
     */
    @CheckReturnValue
    public static ValuePredicate eq(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.eq(varPattern);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value does not equal the specified value
     */
    @CheckReturnValue
    public static ValuePredicate neq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.neq(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value does not equal the specified value
     */
    @CheckReturnValue
    public static ValuePredicate neq(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.neq(varPattern);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly greater than the specified value
     */
    @CheckReturnValue
    public static ValuePredicate gt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gt(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value is strictly greater than the specified value
     */
    @CheckReturnValue
    public static ValuePredicate gt(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.gt(varPattern);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is greater or equal to the specified value
     */
    @CheckReturnValue
    public static ValuePredicate gte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gte(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value is greater or equal to the specified value
     */
    @CheckReturnValue
    public static ValuePredicate gte(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.gte(varPattern);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly less than the specified value
     */
    @CheckReturnValue
    public static ValuePredicate lt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lt(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value is strictly less than the specified value
     */
    @CheckReturnValue
    public static ValuePredicate lt(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.lt(varPattern);
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is less or equal to the specified value
     */
    @CheckReturnValue
    public static ValuePredicate lte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lte(value);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that is true when a value is less or equal to the specified value
     */
    @CheckReturnValue
    public static ValuePredicate lte(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.lte(varPattern);
    }

    /**
     * @param pattern a regex pattern
     * @return a predicate that returns true when a value matches the given regular expression
     */
    @CheckReturnValue
    public static ValuePredicate regex(String pattern) {
        Objects.requireNonNull(pattern);
        return Predicates.regex(pattern);
    }

    /**
     * @param substring a substring to match
     * @return a predicate that returns true when a value contains the given substring
     */
    @CheckReturnValue
    public static ValuePredicate contains(String substring) {
        Objects.requireNonNull(substring);
        return Predicates.contains(substring);
    }

    /**
     * @param varPattern the variable pattern representing a resource
     * @return a predicate that returns true when a value contains the given substring
     */
    @CheckReturnValue
    public static ValuePredicate contains(Statement varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.contains(varPattern);
    }

}
