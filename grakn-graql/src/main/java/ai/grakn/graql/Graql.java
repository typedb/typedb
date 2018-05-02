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

package ai.grakn.graql;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.analytics.ComputeQueryBuilder;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import ai.grakn.graql.internal.query.predicate.Predicates;
import ai.grakn.graql.internal.util.AdminConverter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.CheckReturnValue;
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
    @CheckReturnValue
    public static QueryBuilder withoutGraph() {
        return new QueryBuilderImpl();
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @CheckReturnValue
    public static Match match(Pattern... patterns) {
        return withoutGraph().match(patterns);
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @CheckReturnValue
    public static Match match(Collection<? extends Pattern> patterns) {
        return withoutGraph().match(patterns);
    }

    /**
     * @param varPatterns an array of variable patterns to insert into the graph
     * @return an insert query that will insert the given variable patterns into the graph
     */
    @CheckReturnValue
    public static InsertQuery insert(VarPattern... varPatterns) {
        return withoutGraph().insert(varPatterns);
    }

    /**
     * @param varPatterns a collection of variable patterns to insert into the graph
     * @return an insert query that will insert the given variable patterns into the graph
     */
    @CheckReturnValue
    public static InsertQuery insert(Collection<? extends VarPattern> varPatterns) {
        return withoutGraph().insert(varPatterns);
    }

    /**
     * @param varPatterns an array of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static DefineQuery define(VarPattern... varPatterns) {
        return withoutGraph().define(varPatterns);
    }

    /**
     * @param varPatterns a collection of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static DefineQuery define(Collection<? extends VarPattern> varPatterns) {
        return withoutGraph().define(varPatterns);
    }

    /**
     * @param varPatterns an array of {@link VarPattern}s undefining {@link SchemaConcept}s
     * @return a {@link UndefineQuery} that will remove the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static UndefineQuery undefine(VarPattern... varPatterns) {
        return withoutGraph().undefine(varPatterns);
    }

    /**
     * @param varPatterns a collection of {@link VarPattern}s undefining {@link SchemaConcept}s
     * @return a {@link UndefineQuery} that will remove the changes described in the {@code patterns}
     */
    @CheckReturnValue
    public static UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
        return withoutGraph().undefine(varPatterns);
    }

    /**
     * @return a compute query builder without a specified graph
     */
    @CheckReturnValue
    public static ComputeQueryBuilder compute() {
        return withoutGraph().compute();
    }

    /**
     * Get a {@link QueryParser} for parsing queries from strings
     */
    public static QueryParser parser() {
        return withoutGraph().parser();
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    @CheckReturnValue
    public static <T extends Query<?>> T parse(String queryString) {
        return withoutGraph().parse(queryString);
    }

    // PATTERNS AND VARS

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    @CheckReturnValue
    public static Var var(String name) {
        return Patterns.var(name);
    }

    /**
     * @return a new, anonymous query variable
     */
    @CheckReturnValue
    public static Var var() {
        return Patterns.var();
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static VarPattern label(Label label) {
        return var().label(label);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static VarPattern label(String label) {
        return var().label(label);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Pattern and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Pattern and(Collection<? extends Pattern> patterns) {
        Collection<PatternAdmin> patternAdmins = AdminConverter.getPatternAdmins(patterns);
        return Patterns.conjunction(Sets.newHashSet(patternAdmins));
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    public static Pattern or(Pattern... patterns) {
        return or(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    public static Pattern or(Collection<? extends Pattern> patterns) {
        // Simplify representation when there is only one alternative
        if (patterns.size() == 1) {
            return Iterables.getOnlyElement(patterns);
        }

        Collection<PatternAdmin> patternAdmins = AdminConverter.getPatternAdmins(patterns);
        return Patterns.disjunction(Sets.newHashSet(patternAdmins));
    }


    // AGGREGATES

    /**
     * Create an aggregate that will check if there are any results
     */
    @CheckReturnValue
    public static Aggregate<Object, Boolean> ask() {
        return Aggregates.ask();
    }

    /**
     * Create an aggregate that will count the results of a query.
     */
    @CheckReturnValue
    public static Aggregate<Object, Long> count() {
        return Aggregates.count();
    }

    /**
     * Create an aggregate that will sum the values of a variable.
     */
    @CheckReturnValue
    public static Aggregate<Answer, Number> sum(String var) {
        return Aggregates.sum(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static <T extends Comparable<T>> Aggregate<Answer, Optional<T>> max(String var) {
        return Aggregates.max(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static <T extends Comparable<T>> Aggregate<Answer, Optional<T>> min(String var) {
        return Aggregates.min(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     * @param var the variable to find the mean of
     */
    @CheckReturnValue
    public static Aggregate<Answer, Optional<Double>> mean(String var) {
        return Aggregates.mean(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     * @param var the variable to find the median of
     */
    @CheckReturnValue
    public static Aggregate<Answer, Optional<Number>> median(String var) {
        return Aggregates.median(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the unbiased sample standard deviation of a variable's values.
     * @param var the variable to find the standard deviation of
     */
    @CheckReturnValue
    public static Aggregate<Answer, Optional<Double>> std(String var) {
        return Aggregates.std(Graql.var(var));
    }

    /**
     * Create an aggregate that will group a query by a variable.
     * @param var the variable to group results by
     */
    @CheckReturnValue
    public static Aggregate<Answer, Map<Concept, List<Answer>>> group(String var) {
        return group(var, Aggregates.list());
    }

    /**
     * Create an aggregate that will group a query by a variable and apply the given aggregate to each group
     * @param var the variable to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T> the type the aggregate returns
     */
    @CheckReturnValue
    public static <T> Aggregate<Answer, Map<Concept, T>> group(
            String var, Aggregate<? super Answer, T> aggregate) {
        return Aggregates.group(Graql.var(var), aggregate);
    }

    /**
     * Create an aggregate that will collect together several named aggregates into a map.
     * @param aggregates the aggregates to join together
     * @param <S> the type that the query returns
     * @param <T> the type that each aggregate returns
     */
    @CheckReturnValue
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
    @CheckReturnValue
    public static <S, T> Aggregate<S, Map<String, T>> select(Set<NamedAggregate<? super S, ? extends T>> aggregates) {
        return Aggregates.select(ImmutableSet.copyOf(aggregates));
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
    public static ValuePredicate eq(VarPattern varPattern) {
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
    public static ValuePredicate neq(VarPattern varPattern) {
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
    public static ValuePredicate gt(VarPattern varPattern) {
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
    public static ValuePredicate gte(VarPattern varPattern) {
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
    public static ValuePredicate lt(VarPattern varPattern) {
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
    public static ValuePredicate lte(VarPattern varPattern) {
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
    public static ValuePredicate contains(VarPattern varPattern) {
        Objects.requireNonNull(varPattern);
        return Predicates.contains(varPattern.admin());
    }
}
