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
import grakn.core.graql.parser.Parser;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Disjunction;
import grakn.core.graql.query.pattern.Negation;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.StatementInstance;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementAttribute;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementRelation;
import grakn.core.graql.query.pattern.statement.StatementType;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.predicate.Predicates;
import grakn.core.graql.query.predicate.ValuePredicate;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.query.ComputeQuery.Method;

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
    public static <T extends Query> T parse(String queryString) {
        return parser.parseQueryEOF(queryString);
    }

    @CheckReturnValue
    public static <T extends Query> Stream<T> parseList(String queryString) {
        return parser.parseQueryListEOF(queryString);
    }

    @CheckReturnValue
    public static Pattern parsePattern(String pattern) {
        return parser.parsePatternEOF(pattern);
    }

    @CheckReturnValue
    public static List<Pattern> parsePatternList(String pattern) {
        return parser.parsePatternListEOF(pattern).collect(Collectors.toList());
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
        return new MatchClause(and(Collections.unmodifiableSet(new LinkedHashSet<>(patterns))));
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

    // PATTERNS


    /**
     * @return a new statement with an anonymous Variable
     */
    @CheckReturnValue
    public static Statement var() {
        return var(new Variable());
    }

    /**
     * @param name the name of the variable
     * @return a new statement with a variable of a given name
     */
    @CheckReturnValue
    public static Statement var(String name) {
        return var(new Variable(name));
    }

    /**
     * @param var a variable to create a statement
     * @return a new statement with a provided variable
     */
    @CheckReturnValue
    public static Statement var(Variable var) {
        return new Statement(var);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static StatementType type(String label) {
        return var(new Variable(false)).type(label);
    }

    @CheckReturnValue
    public static StatementRelation rel(String player) {
        return var(new Variable(false)).rel(player);
    }

    @CheckReturnValue
    public static StatementRelation rel(String role, String player) {
        return var(new Variable(false)).rel(role, player);
    }

    public static StatementRelation rel(Statement role, Statement player) {
        return var(new Variable(false)).rel(role, player);
    }

    @CheckReturnValue
    public static StatementAttribute val(Object value) {
        return var(new Variable(false)).val(Graql.eq(value));
    }

    @CheckReturnValue
    public static StatementAttribute val(ValuePredicate predicate) {
        return var(new Variable(false)).val(new ValueProperty(predicate));
    }


    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Conjunction<?> and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Conjunction<?> and(Collection<? extends Pattern> patterns) {
        return and(new LinkedHashSet<>(patterns));
    }

    public static <T extends Pattern> Conjunction<T> and(Set<T> patterns) {
        return new Conjunction<>(patterns);
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
            return patterns.iterator().next();
        }

        return or(new LinkedHashSet<>(patterns));
    }

    public static <T extends Pattern> Disjunction<T> or(Set<T> patterns) {
        return new Disjunction<>(patterns);
    }

    /**
     *
     * @param pattern a patterns to a negate
     * @return a pattern that will match when no contained pattern matches
     */
    @CheckReturnValue
    public static Negation<Pattern> not(Pattern pattern) {
        return new Negation<>(pattern);
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
    public static ValuePredicate like(String pattern) {
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
