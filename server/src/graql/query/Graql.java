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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.Value;
import grakn.core.graql.parser.QueryParser;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.query.aggregate.Aggregates;
import grakn.core.graql.query.predicate.Predicates;
import grakn.core.graql.internal.util.AdminConverter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static grakn.core.graql.query.Syntax.Compute.Method;
import static java.util.stream.Collectors.toSet;

/**
 * Main class containing static methods for creating Graql queries.
 *
 * It is recommended you statically import these methods.
 *
 */
public class Graql {

    private Graql() {}

    // QUERY BUILDING

    /**
     * @return a query builder without a specified graph
     */
    @CheckReturnValue
    public static QueryBuilder withoutGraph() {
        return new QueryBuilder();
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

    @CheckReturnValue
    public static <T extends Answer> ComputeQuery<T> compute(Method<T> method) {
        return withoutGraph().compute(method);
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
     * Create an aggregate that will count the results of a query.
     */
    @CheckReturnValue
    public static Aggregate<Value> count(String... vars) {
        return Aggregates.count(Arrays.stream(vars).map(Graql::var).collect(toSet()));
    }

    /**
     * Create an aggregate that will sum the values of a variable.
     */
    @CheckReturnValue
    public static Aggregate<Value> sum(String var) {
        return Aggregates.sum(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the minimum of a variable's values.
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> min(String var) {
        return Aggregates.min(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the maximum of a variable's values.
     * @param var the variable to find the maximum of
     */
    @CheckReturnValue
    public static Aggregate<Value> max(String var) {
        return Aggregates.max(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the mean of a variable's values.
     * @param var the variable to find the mean of
     */
    @CheckReturnValue
    public static Aggregate<Value> mean(String var) {
        return Aggregates.mean(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the median of a variable's values.
     * @param var the variable to find the median of
     */
    @CheckReturnValue
    public static Aggregate<Value> median(String var) {
        return Aggregates.median(Graql.var(var));
    }

    /**
     * Create an aggregate that will find the unbiased sample standard deviation of a variable's values.
     * @param var the variable to find the standard deviation of
     */
    @CheckReturnValue
    public static Aggregate<Value> std(String var) {
        return Aggregates.std(Graql.var(var));
    }

    /**
     * Create an aggregate that will group a query by a variable.
     * @param var the variable to group results by
     */
    @CheckReturnValue
    public static Aggregate<AnswerGroup<ConceptMap>> group(String var) {
        return group(var, Aggregates.list());
    }

    /**
     * Create an aggregate that will group a query by a variable and apply the given aggregate to each group
     * @param var the variable to group results by
     * @param aggregate the aggregate to apply to each group
     * @param <T> the type the aggregate returns
     */
    @CheckReturnValue
    public static <T extends Answer> Aggregate<AnswerGroup<T>> group(String var, Aggregate<T> aggregate) {
        return Aggregates.group(Graql.var(var), aggregate);
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
