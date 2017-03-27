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

import ai.grakn.graql.VarName;
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

    Atomic copy();

    default boolean isAtom(){ return false;}
    default boolean isPredicate(){ return false;}

    /**
     * @return true if atom alpha-equivalent
     */
    boolean isEquivalent(Object obj);

    /**
     * @return equivalence hash code
     */
    int equivalenceHashCode();

    /**
     * @return true if the variable name is user defined
     */
    default boolean isUserDefinedName(){ return false;}

    /**
     * @return true if the atom can be resolved by a rule (atom exists in one of the rule's head)
     */
    default boolean isRuleResolvable(){ return false;}
    /**
     * @return true if the atom can form an atomic query
     */
    default boolean isSelectable(){ return false;}

    /**
     * @return true if atom is recursive
     */
    default boolean isRecursive(){ return false;}

    /**
     * @param name variable name
     * @return true if atom contains an occurrence of the variable name
     */
    default boolean containsVar(VarName name){ return false;}

    /**
     * @return the corresponding base pattern
     * */
    PatternAdmin getPattern();

    /**
     * @return the base pattern combined with possible predicate patterns
     */
    PatternAdmin getCombinedPattern();

    /**
     * @return the query the atom is contained in
     */
    ReasonerQuery getParentQuery();

    /**
     * @param q query this atom is supposed to belong to
     */
    void setParentQuery(ReasonerQuery q);

    Unifier getUnifier(Atomic parentAtom);

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * if capture occurs it is marked with a "capture-><name of the captured occurrence>" name
     * @param unifier contain variable mappings to be applied
     */
    void unify(Unifier unifier);

    VarName getVarName();

    /**
     * @return all addressable variable names in the atom
     */
    Set<VarName> getVarNames();
}
