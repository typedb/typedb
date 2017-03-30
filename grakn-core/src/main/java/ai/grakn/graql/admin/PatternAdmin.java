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

package ai.grakn.graql.admin;

import ai.grakn.graql.Pattern;
import ai.grakn.graql.VarName;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Admin class for inspecting and manipulating a Pattern
 *
 * @author Felix Chapman
 */
public interface PatternAdmin extends Pattern {
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
    Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm();

    /**
     * Get all common, user-defined variable names in the pattern.
     */
    Set<VarName> commonVarNames();

    /**
     * @return true if this Pattern.Admin is a Conjunction
     */
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Disjunction
     */
    default boolean isConjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Var.Admin
     */
    default boolean isVar() {
        return false;
    }

    /**
     * @return this Pattern.Admin as a Disjunction, if it is one.
     */
    default Disjunction<?> asDisjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern.Admin as a Conjunction, if it is one.
     */
    default Conjunction<?> asConjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern.Admin as a Var.Admin, if it is one.
     */
    default VarAdmin asVar() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return all variables referenced in the pattern
     */
    default Set<VarAdmin> getVars() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(toSet());
    }
}
