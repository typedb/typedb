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
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.StatementInstance;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementAttribute;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementRelation;
import grakn.core.graql.query.pattern.statement.StatementType;
import grakn.core.graql.query.pattern.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
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

    @CheckReturnValue
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

    // Pattern Builder Methods

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

    @CheckReturnValue
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

    @CheckReturnValue
    public static <T extends Pattern> Disjunction<T> or(Set<T> patterns) {
        return new Disjunction<>(patterns);
    }

    /**
     * @param pattern a patterns to a negate
     * @return a pattern that will match when no contained pattern matches
     */
    @CheckReturnValue
    public static Negation<Pattern> not(Pattern pattern) {
        return new Negation<>(pattern);
    }

    // Statement Builder Methods

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

    @CheckReturnValue
    private static Statement hiddenVar() {
        return var(new Variable(false));
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static StatementType type(String label) {
        return hiddenVar().type(label);
    }

    @CheckReturnValue
    public static StatementRelation rel(String player) {
        return hiddenVar().rel(player);
    }

    @CheckReturnValue
    public static StatementRelation rel(String role, String player) {
        return hiddenVar().rel(role, player);
    }

    @CheckReturnValue
    public static StatementRelation rel(Statement role, Statement player) {
        return hiddenVar().rel(role, player);
    }

    // Attribute Statement Builder Methods

    // Attribute value assignment property

    @CheckReturnValue
    public static StatementAttribute val(long value) {
        return hiddenVar().val(value);
    }

    @CheckReturnValue
    public static StatementAttribute val(double value) {
        return hiddenVar().val(value);
    }

    @CheckReturnValue
    public static StatementAttribute val(boolean value) {
        return hiddenVar().val(value);
    }

    @CheckReturnValue
    public static StatementAttribute val(String value) {
        return hiddenVar().val(value);
    }

    @CheckReturnValue
    public static StatementAttribute val(LocalDateTime value) {
        return hiddenVar().val(value);
    }

    // Attribute value equality property

    @CheckReturnValue
    public static StatementAttribute eq(long value) {
        return hiddenVar().eq(value);
    }

    @CheckReturnValue
    public static StatementAttribute eq(double value) {
        return hiddenVar().eq(value);
    }

    @CheckReturnValue
    public static StatementAttribute eq(boolean value) {
        return hiddenVar().eq(value);
    }

    @CheckReturnValue
    public static StatementAttribute eq(String value) {
        return hiddenVar().eq(value);
    }

    @CheckReturnValue
    public static StatementAttribute eq(LocalDateTime value) {
        return hiddenVar().eq(value);
    }

    @CheckReturnValue
    public static StatementAttribute eq(Statement variable) {
        return hiddenVar().eq(variable);
    }

    // Attribute value inequality property

    @CheckReturnValue
    public static StatementAttribute neq(long value) {
        return hiddenVar().neq(value);
    }

    @CheckReturnValue
    public static StatementAttribute neq(double value) {
        return hiddenVar().neq(value);
    }

    @CheckReturnValue
    public static StatementAttribute neq(boolean value) {
        return hiddenVar().neq(value);
    }

    @CheckReturnValue
    public static StatementAttribute neq(String value) {
        return hiddenVar().neq(value);
    }

    @CheckReturnValue
    public static StatementAttribute neq(LocalDateTime value) {
        return hiddenVar().neq(value);
    }

    @CheckReturnValue
    public static StatementAttribute neq(Statement variable) {
        return hiddenVar().neq(variable);
    }

    // Attribute value greater-than property

    @CheckReturnValue
    public static StatementAttribute gt(long value) {
        return hiddenVar().gt(value);
    }

    @CheckReturnValue
    public static StatementAttribute gt(double value) {
        return hiddenVar().gt(value);
    }

    @CheckReturnValue
    public static StatementAttribute gt(boolean value) {
        return hiddenVar().gt(value);
    }

    @CheckReturnValue
    public static StatementAttribute gt(String value) {
        return hiddenVar().gt(value);
    }

    @CheckReturnValue
    public static StatementAttribute gt(LocalDateTime value) {
        return hiddenVar().gt(value);
    }

    @CheckReturnValue
    public static StatementAttribute gt(Statement variable) {
        return hiddenVar().gt(variable);
    }

    // Attribute value greater-than-or-equals property

    @CheckReturnValue
    public static StatementAttribute gte(long value) {
        return hiddenVar().gte(value);
    }

    @CheckReturnValue
    public static StatementAttribute gte(double value) {
        return hiddenVar().gte(value);
    }

    @CheckReturnValue
    public static StatementAttribute gte(boolean value) {
        return hiddenVar().gte(value);
    }

    @CheckReturnValue
    public static StatementAttribute gte(String value) {
        return hiddenVar().gte(value);
    }

    @CheckReturnValue
    public static StatementAttribute gte(LocalDateTime value) {
        return hiddenVar().gte(value);
    }

    @CheckReturnValue
    public static StatementAttribute gte(Statement variable) {
        return hiddenVar().gte(variable);
    }

    // Attribute value less-than property

    @CheckReturnValue
    public static StatementAttribute lt(long value) {
        return hiddenVar().lt(value);
    }

    @CheckReturnValue
    public static StatementAttribute lt(double value) {
        return hiddenVar().lt(value);
    }

    @CheckReturnValue
    public static StatementAttribute lt(boolean value) {
        return hiddenVar().lt(value);
    }

    @CheckReturnValue
    public static StatementAttribute lt(String value) {
        return hiddenVar().lt(value);
    }

    @CheckReturnValue
    public static StatementAttribute lt(LocalDateTime value) {
        return hiddenVar().lt(value);
    }

    @CheckReturnValue
    public static StatementAttribute lt(Statement variable) {
        return hiddenVar().lt(variable);
    }

    // Attribute value less-than-or-equals property

    @CheckReturnValue
    public static StatementAttribute lte(long value) {
        return hiddenVar().lte(value);
    }

    @CheckReturnValue
    public static StatementAttribute lte(double value) {
        return hiddenVar().lte(value);
    }

    @CheckReturnValue
    public static StatementAttribute lte(boolean value) {
        return hiddenVar().lte(value);
    }

    @CheckReturnValue
    public static StatementAttribute lte(String value) {
        return hiddenVar().lte(value);
    }

    @CheckReturnValue
    public static StatementAttribute lte(LocalDateTime value) {
        return hiddenVar().lte(value);
    }

    @CheckReturnValue
    public static StatementAttribute lte(Statement variable) {
        return hiddenVar().lte(variable);
    }

    // Attribute value contains (in String) property

    @CheckReturnValue
    public static StatementAttribute contains(String value) {
        return hiddenVar().contains(value);
    }

    @CheckReturnValue
    public static StatementAttribute contains(Statement variable) {
        return hiddenVar().contains(variable);
    }

    // Attribute value regex property

    @CheckReturnValue
    public static StatementAttribute like(String value) {
        return hiddenVar().like(value);
    }
}
