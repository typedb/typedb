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

package ai.grakn.graql;

import ai.grakn.concept.Concept;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import ai.grakn.graql.internal.query.predicate.Predicates;
import ai.grakn.graql.internal.util.AdminConverter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Main class containing static methods for creating Graql queries.
 *
 * It is recommended you statically import these methods.
 *
 * @author Felix Chapman
 */
public class Graql {

    private Graql() {}

    // QUERY BUILDING

    /**
     * @return a query builder without a specified graph
     */
    public static QueryBuilder withoutGraph() {
        return new QueryBuilderImpl();
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public static MatchQuery match(Pattern... patterns) {
        return withoutGraph().match(patterns);
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public static MatchQuery match(Collection<? extends Pattern> patterns) {
        return withoutGraph().match(patterns);
    }

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public static InsertQuery insert(Var... vars) {
        return withoutGraph().insert(vars);
    }

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public static InsertQuery insert(Collection<? extends Var> vars) {
        return withoutGraph().insert(vars);
    }

    /**
     * @return a compute query builder without a specified graph
     */
    public static ComputeQueryBuilder compute() {
        return withoutGraph().compute();
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    public static List<Pattern> parsePatterns(String patternsString) {
        return withoutGraph().parsePatterns(patternsString);
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    public static <T extends Query<?>> T parse(String queryString) {
        return withoutGraph().parse(queryString);
    }

    /**
     * @param queryString a string representing several queries
     * @return a list of queries
     */
    public static List<Query<?>> parseList(String queryString) {
        return withoutGraph().parseList(queryString);
    }

    // TEMPLATING

    /**
     * @param template a string representing a templated graql query
     * @param data data to use in template
     * @return a query, the type will depend on the type indicated in the template
     */
    public static <T extends Query<?>> List<T> parseTemplate(String template, Map<String, Object> data){
        return withoutGraph().parseTemplate(template, data);
    }

    // PATTERNS AND VARS

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    public static Var var(String name) {
        return var(VarName.of(name));
    }

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    public static Var var(VarName name) {
        return Patterns.var(Objects.requireNonNull(name));
    }

    /**
     * @return a new, anonymous query variable
     */
    public static Var var() {
        return Patterns.var();
    }

    /**
     * @param label the label of a concept
     * @return a query variable that identifies a concept by label
     */
    public static Var label(TypeLabel label) {
        return var().label(label);
    }

    /**
     * @param label the label of a concept
     * @return a query variable that identifies a concept by label
     */
    public static Var label(String label) {
        return var().label(label);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    public static Pattern and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    public static Pattern and(Collection<? extends Pattern> patterns) {
        Collection<PatternAdmin> patternAdmins = AdminConverter.getPatternAdmins(patterns);
        return Patterns.conjunction(Sets.newHashSet(patternAdmins));
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    public static Pattern or(Pattern... patterns) {
        return or(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    public static Pattern or(Collection<? extends Pattern> patterns) {
        Collection<PatternAdmin> patternAdmins = AdminConverter.getPatternAdmins(patterns);
        return Patterns.disjunction(Sets.newHashSet(patternAdmins));
    }


    // AGGREGATES

    /**
     * Create an aggregate that will count the results of a query.
     */
    public static Aggregate<Object, Long> count() {
        return Aggregates.count();
    }

    /**
     * Create an aggregate that will sum the values of a variable.
     */
    public static Aggregate<Map<VarName, Concept>, Number> sum(String name) {
        return Aggregates.sum(VarName.of(name));
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     * @param name the variable to find the maximum of
     */
    public static <T extends Comparable<T>> Aggregate<Map<VarName, Concept>, Optional<T>> max(String name) {
        return Aggregates.max(VarName.of(name));
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     * @param name the variable to find the maximum of
     */
    public static <T extends Comparable<T>> Aggregate<Map<VarName, Concept>, Optional<T>> min(String name) {
        return Aggregates.min(VarName.of(name));
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     * @param name the variable to find the mean of
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Double>> mean(String name) {
        return Aggregates.mean(VarName.of(name));
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     * @param name the variable to find the median of
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Number>> median(String name) {
        return Aggregates.median(VarName.of(name));
    }

    /**
     * Create an aggregate that will find the unbiased sample standard deviation of a variable's values.
     * @param name the variable to find the standard deviation of
     */
    public static Aggregate<Map<VarName, Concept>, Optional<Double>> std(String name) {
        return Aggregates.std(VarName.of(name));
    }

    /**
     * Create an aggregate that will group a query by a variable name.
     * @param varName the variable name to group results by
     */
    public static Aggregate<Map<VarName, Concept>, Map<Concept, List<Map<VarName, Concept>>>> group(String varName) {
        return group(varName, Aggregates.list());
    }

    /**
     * Create an aggregate that will group a query by a variable name and apply the given aggregate to each group
     * @param varName the variable name to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T> the type the aggregate returns
     */
    public static <T> Aggregate<Map<VarName, Concept>, Map<Concept, T>> group(
            String varName, Aggregate<? super Map<VarName, Concept>, T> aggregate) {
        return Aggregates.group(VarName.of(varName), aggregate);
    }

    /**
     * Create an aggregate that will collect together several named aggregates into a map.
     * @param aggregates the aggregates to join together
     * @param <S> the type that the query returns
     * @param <T> the type that each aggregate returns
     */
    @SafeVarargs
    public static <S, T> Aggregate<S, Map<String, T>> select(NamedAggregate<? super S, ? extends T>... aggregates) {
        return select(ImmutableSet.copyOf(aggregates));
    }

    /**
     * Create an aggregate that will collect together several named aggregates into a map.
     * @param aggregates the aggregates to join together
     * @param <S> the type that the query returns
     * @param <T> the type that each aggregate returns
     */
    public static <S, T> Aggregate<S, Map<String, T>> select(Set<NamedAggregate<? super S, ? extends T>> aggregates) {
        return Aggregates.select(ImmutableSet.copyOf(aggregates));
    }


    // PREDICATES

    /**
     * @param value the value
     * @return a predicate that is true when a value equals the specified value
     */
    public static ValuePredicate eq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.eq(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value equals the specified value
     */
    public static ValuePredicate eq(Var var) {
        Objects.requireNonNull(var);
        return Predicates.eq(var.admin());
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value does not equal the specified value
     */
    public static ValuePredicate neq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.neq(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value does not equal the specified value
     */
    public static ValuePredicate neq(Var var) {
        Objects.requireNonNull(var);
        return Predicates.neq(var.admin());
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly greater than the specified value
     */
    public static ValuePredicate gt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gt(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value is strictly greater than the specified value
     */
    public static ValuePredicate gt(Var var) {
        Objects.requireNonNull(var);
        return Predicates.gt(var.admin());
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is greater or equal to the specified value
     */
    public static ValuePredicate gte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gte(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value is greater or equal to the specified value
     */
    public static ValuePredicate gte(Var var) {
        Objects.requireNonNull(var);
        return Predicates.gte(var.admin());
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is strictly less than the specified value
     */
    public static ValuePredicate lt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lt(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value is strictly less than the specified value
     */
    public static ValuePredicate lt(Var var) {
        Objects.requireNonNull(var);
        return Predicates.lt(var.admin());
    }

    /**
     * @param value the value
     * @return a predicate that is true when a value is less or equal to the specified value
     */
    public static ValuePredicate lte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lte(value);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that is true when a value is less or equal to the specified value
     */
    public static ValuePredicate lte(Var var) {
        Objects.requireNonNull(var);
        return Predicates.lte(var.admin());
    }

    /**
     * @param pattern a regex pattern
     * @return a predicate that returns true when a value matches the given regular expression
     */
    public static ValuePredicate regex(String pattern) {
        Objects.requireNonNull(pattern);
        return Predicates.regex(pattern);
    }

    /**
     * @param substring a substring to match
     * @return a predicate that returns true when a value contains the given substring
     */
    public static ValuePredicate contains(String substring) {
        Objects.requireNonNull(substring);
        return Predicates.contains(substring);
    }

    /**
     * @param var the variable representing a resource
     * @return a predicate that returns true when a value contains the given substring
     */
    public static ValuePredicate contains(Var var) {
        Objects.requireNonNull(var);
        return Predicates.contains(var.admin());
    }
}
