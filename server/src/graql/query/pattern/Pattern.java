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

package grakn.core.graql.query.pattern;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Label;
import grakn.core.graql.parser.Parser;
import grakn.core.graql.query.Graql;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toSet;

/**
 * A pattern describing a subgraph.
 * A Pattern can describe an entire graph, or just a single concept.
 * For example, {@code var("x").isa("movie")} is a pattern representing things that are movies.
 * A pattern can also be a conjunction: {@code and(var("x").isa("movie"), var("x").value("Titanic"))}, or a disjunction:
 * {@code or(var("x").isa("movie"), var("x").isa("tv-show"))}. These can be used to combine other patterns together
 * into larger patterns.
 */
public interface Pattern {

    AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);
    Parser parser = new Parser();

    static Pattern parse(String pattern) {
        return parser.parsePattern(pattern);
    }

    static List<Pattern> parseList(String pattern) {
        return parser.parsePatterns(pattern);
    }

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    @CheckReturnValue
    static Variable var(String name) {
        return new Variable(name, Variable.Type.UserDefined);
    }

    /**
     * @return a new, anonymous query variable
     */
    @CheckReturnValue
    static Variable var() {
        return new Variable(Long.toString(counter.getAndIncrement()), Variable.Type.Generated);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    static Statement label(String label) {
        return var().label(label);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    static Statement label(Label label) {
        return var().label(label);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    static Pattern and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    static Pattern and(Collection<? extends Pattern> patterns) {
        return and(Sets.newHashSet(patterns));
    }

    static <T extends Pattern> Conjunction<T> and(Set<T> patterns) {
        return new Conjunction<>(patterns);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    static Pattern or(Pattern... patterns) {
        return or(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    static Pattern or(Collection<? extends Pattern> patterns) {
        // Simplify representation when there is only one alternative
        if (patterns.size() == 1) {
            return Iterables.getOnlyElement(patterns);
        }

        return or(Sets.newHashSet(patterns));
    }

    static <T extends Pattern> Disjunction<T> or(Set<T> patterns) {
        return new Disjunction<>(patterns);
    }

    /**
     * Get all common, user-defined {@link Variable}s in the {@link Pattern}.
     */
    @CheckReturnValue
    Set<Variable> variables();

    /**
     * @return all variables referenced in the pattern
     */
    @CheckReturnValue
    default Set<Statement> statements() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(toSet());
    }

    /**
     * @return this {@link Pattern} as a {@link Statement}, if it is one.
     */
    @CheckReturnValue
    default Statement asStatement() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this {@link Pattern} as a {@link Disjunction}, if it is one.
     */
    @CheckReturnValue
    default Disjunction<?> asDisjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this {@link Pattern} as a {@link Conjunction}, if it is one.
     */
    @CheckReturnValue
    default Conjunction<?> asConjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return true if this {@link Pattern} is a {@link Statement}
     */
    @CheckReturnValue
    default boolean isStatement() {
        return false;
    }

    /**
     * @return true if this {@link Pattern} is a {@link Conjunction}
     */
    @CheckReturnValue
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this {@link Pattern} is a {@link Disjunction}
     */
    @CheckReturnValue
    default boolean isConjunction() {
        return false;
    }

    /**
     * Get the disjunctive normal form of this pattern group.
     * This means the pattern group will be transformed into a number of conjunctive patterns, where each is disjunct.
     *
     * e.g.
     * p = (A or B) and (C or D)
     * p.getDisjunctiveNormalForm() = (A and C) or (A and D) or (B and C) or (B and D)
     *
     * @return the pattern group in disjunctive normal form
     */
    @CheckReturnValue
    Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm();
}
