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

import grakn.core.graql.parser.Parser;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.CheckReturnValue;

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

    Parser parser = new Parser();

    static Pattern parse(String pattern) {
        return parser.parsePattern(pattern);
    }

    static List<Pattern> parseList(String pattern) {
        return parser.parsePatterns(pattern);
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
     * @return this {@link Pattern} as a {@link Negation}, if it is one.
     */
    @CheckReturnValue
    default Negation<?> asNegation() {
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
     * @return true if this {@link Pattern} is a {@link Negation}
     */
    @CheckReturnValue
    default boolean isNegation() {
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

    /**
     * @return this pattern negated
     */
    @CheckReturnValue
    Pattern negate();

    /**
     * @return true if this pattern contains only positive patterns
     */
    @CheckReturnValue
    default boolean isPositive(){
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(p -> p.getPatterns().stream())
                .allMatch(Pattern::isPositive);
    }
}
