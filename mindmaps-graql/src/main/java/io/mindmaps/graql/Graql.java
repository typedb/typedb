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
 *
 */

package io.mindmaps.graql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.pattern.Patterns;
import io.mindmaps.graql.internal.query.aggregate.Aggregates;
import io.mindmaps.graql.internal.query.predicate.Predicates;
import io.mindmaps.graql.internal.util.AdminConverter;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
     * @param inputStream a stream representing a list of patterns
     * @return a stream of patterns
     */
    public static Stream<Pattern> parsePatterns(InputStream inputStream) {
        return withoutGraph().parsePatterns(inputStream);
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

    // TEMPLATING

    /**
     * @param template a string representing a templated graql query
     * @param data data to use in template
     * @return a resolved graql query
     */
    public static String parseTemplate(String template, Map<String, Object> data){
        return withoutGraph().parseTemplate(template, data);
    }

    // PATTERNS AND VARS

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    public static Var var(String name) {
        return Patterns.var(Objects.requireNonNull(name));
    }

    /**
     * @return a new, anonymous query variable
     */
    public static Var var() {
        return Patterns.var();
    }

    /**
     * @param id the id of a concept
     * @return a query variable that identifies a concept by id
     */
    public static Var id(String id) {
        return var().id(id);
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
        Conjunction<PatternAdmin> conjunction = Patterns.conjunction(Sets.newHashSet(patternAdmins));

        return conjunction;
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
        Disjunction<PatternAdmin> disjunction = Patterns.disjunction(Sets.newHashSet(patternAdmins));

        return disjunction;
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
    public static Aggregate<Map<String, Concept>, Number> sum(String varName) {
        return Aggregates.sum(varName);
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     * @param varName the variable to find the maximum of
     */
    public static Aggregate<Map<String, Concept>, Optional<?>> max(String varName) {
        return Aggregates.max(varName);
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     * @param varName the variable to find the maximum of
     */
    public static Aggregate<Map<String, Concept>, Optional<?>> min(String varName) {
        return Aggregates.min(varName);
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     * @param varName the variable to find the mean of
     */
    public static Aggregate<Map<String, Concept>, Optional<Double>> average(String varName) {
        return Aggregates.average(varName);
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     * @param varName the variable to find the median of
     */
    public static Aggregate<Map<String, Concept>, Optional<Number>> median(String varName) {
        return Aggregates.median(varName);
    }

    /**
     * Create an aggregate that will group a query by a variable name.
     * @param varName the variable name to group results by
     */
    public static Aggregate<Map<String, Concept>, Map<Concept, List<Map<String, Concept>>>> group(String varName) {
        return group(varName, Aggregates.list());
    }

    /**
     * Create an aggregate that will group a query by a variable name and apply the given aggregate to each group
     * @param varName the variable name to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T> the type the aggregate returns
     */
    public static <T> Aggregate<Map<String, Concept>, Map<Concept, T>> group(
            String varName, Aggregate<? super Map<String, Concept>, T> aggregate) {
        return Aggregates.group(varName, aggregate);
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
     * @return a atom that is true when a value equals the specified value
     */
    public static ValuePredicate eq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.eq(value);
    }

    /**
     * @param value the value
     * @return a atom that is true when a value does not equal the specified value
     */
    public static ValuePredicate neq(Object value) {
        Objects.requireNonNull(value);
        return Predicates.neq(value);
    }

    /**
     * @param value the value
     * @return a atom that is true when a value is strictly greater than the specified value
     */
    public static ValuePredicate gt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gt(value);
    }

    /**
     * @param value the value
     * @return a atom that is true when a value is greater or equal to the specified value
     */
    public static ValuePredicate gte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.gte(value);
    }

    /**
     * @param value the value
     * @return a atom that is true when a value is strictly less than the specified value
     */
    public static ValuePredicate lt(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lt(value);
    }

    /**
     * @param value the value
     * @return a atom that is true when a value is less or equal to the specified value
     */
    public static ValuePredicate lte(Comparable value) {
        Objects.requireNonNull(value);
        return Predicates.lte(value);
    }

    /**
     * @param predicates an array of predicates
     * @return a atom that returns true when all the predicates are true
     */
    public static ValuePredicate all(ValuePredicate predicate, ValuePredicate... predicates) {
        return Arrays.stream(predicates).reduce(predicate, ValuePredicate::and);
    }

    /**
     * @param predicates an array of predicates
     * @return a atom that returns true when any of the predicates are true
     */
    public static ValuePredicate any(ValuePredicate predicate, ValuePredicate... predicates) {
        return Arrays.stream(predicates).reduce(predicate, ValuePredicate::or);
    }

    /**
     * @param pattern a regex pattern
     * @return a atom that returns true when a value matches the given regular expression
     */
    public static ValuePredicate regex(String pattern) {
        Objects.requireNonNull(pattern);
        return Predicates.regex(pattern);
    }

    /**
     * @param substring a substring to match
     * @return a atom that returns true when a value contains the given substring
     */
    public static ValuePredicate contains(String substring) {
        Objects.requireNonNull(substring);
        return Predicates.contains(substring);
    }
}
