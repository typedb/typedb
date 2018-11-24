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

import javax.annotation.CheckReturnValue;
import java.util.Set;

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

    /**
     * Join patterns in a conjunction
     */
    @CheckReturnValue
    Pattern and(Pattern pattern);

    /**
     * Join patterns in a disjunction
     */
    @CheckReturnValue
    Pattern or(Pattern pattern);

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
    Disjunction<Conjunction<VarPattern>> getDisjunctiveNormalForm();

    /**
     * Get all common, user-defined variable names in the pattern.
     */
    @CheckReturnValue
    Set<Var> commonVars();

    /**
     * @return true if this Pattern.Admin is a Conjunction
     */
    @CheckReturnValue
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Disjunction
     */
    @CheckReturnValue
    default boolean isConjunction() {
        return false;
    }

    /**
     * @return true if this {@link Pattern} is a {@link VarPattern}
     */
    @CheckReturnValue
    default boolean isVarPattern() {
        return false;
    }

    /**
     * @return this {@link Pattern} as a {@link VarPattern}, if it is one.
     */
    @CheckReturnValue
    default VarPattern asVarPattern() {
        throw new UnsupportedOperationException();
    }
    /**
     * @return all variables referenced in the pattern
     */
    @CheckReturnValue
    default Set<VarPattern> varPatterns() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(toSet());
    }
}
