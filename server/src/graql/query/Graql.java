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
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.predicate.Predicates;
import grakn.core.graql.query.predicate.ValuePredicate;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
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

    public static <T extends Query> Stream<T> parseList(String queryString) {
        return parser.parseQueryListEOF(queryString);
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
        return new MatchClause(Pattern.and(Collections.unmodifiableSet(new LinkedHashSet<>(patterns))));
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
     * @param regex a regex pattern
     * @return a predicate that returns true when a value matches the given regular expression
     */
    @CheckReturnValue
    public static ValuePredicate like(String regex) {
        Objects.requireNonNull(regex);
        return Predicates.regex(regex);
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
