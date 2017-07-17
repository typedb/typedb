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

import ai.grakn.graql.Var;

import java.util.HashSet;
import javax.annotation.CheckReturnValue;
import java.util.Set;

/**
 *
 * <p>
 * Basic interface for logical atoms used in reasoning.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface Atomic {

    @CheckReturnValue
    Atomic copy();

    @CheckReturnValue
    default boolean isAtom(){ return false;}

    @CheckReturnValue
    default boolean isPredicate(){ return false;}

    /**
     * @return true if atom alpha-equivalent
     */
    @CheckReturnValue
    boolean isEquivalent(Object obj);

    /**
     * @return equivalence hash code
     */
    @CheckReturnValue
    int equivalenceHashCode();

    /**
     * @return true if the variable name is user defined
     */
    @CheckReturnValue
    default boolean isUserDefinedName(){ return false;}

    /**
     * @return true if the atomic can be resolved by a rule (atom exists in one of the rule's head)
     */
    @CheckReturnValue
    default boolean isRuleResolvable(){ return false;}
    /**
     * @return true if the atomic can form an atomic query
     */
    @CheckReturnValue
    default boolean isSelectable(){ return false;}

    /**
     * @return true if the atomic can constitute the head of a rule
     */
    @CheckReturnValue
    default boolean isAllowedToFormRuleHead(){ return false; }

    /**
     * @return error messages indicating ontological inconsistencies of this atomic
     */
    @CheckReturnValue
    default Set<String> validateOntologically(){ return new HashSet<>();}

    /**
     * @return true if atom is recursive
     */
    @CheckReturnValue
    default boolean isRecursive(){ return false;}

    /**
     * @param name variable name
     * @return true if atom contains an occurrence of the variable name
     */
    @CheckReturnValue
    default boolean containsVar(Var name){ return false;}

    /**
     * @return the corresponding base pattern
     * */
    @CheckReturnValue
    PatternAdmin getPattern();

    /**
     * @return the base pattern combined with possible predicate patterns
     */
    @CheckReturnValue
    PatternAdmin getCombinedPattern();

    /**
     * @return the query the atom is contained in
     */
    @CheckReturnValue
    ReasonerQuery getParentQuery();

    /**
     * @param q query this atom is supposed to belong to
     */
    void setParentQuery(ReasonerQuery q);

    @CheckReturnValue
    Var getVarName();

    /**
     * @return all addressable variable names in the atom
     */
    @CheckReturnValue
    Set<Var> getVarNames();
}
