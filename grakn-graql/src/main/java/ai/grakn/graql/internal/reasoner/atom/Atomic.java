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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import java.util.Map;
import java.util.Set;

public interface Atomic extends Cloneable{

    Atomic clone();

    default boolean isAtom(){ return false;}
    default boolean isPredicate(){ return false;}

    /**
     * @return true if atom alpha-equivalent
     */
    boolean isEquivalent(Object obj);
    int equivalenceHashCode();

    default boolean isUserDefinedName(){ return false;}

    /**
     * @return true if the atom can be resolved by a rule (atom exists in one of the rule's head)
     */
    default boolean isRuleResolvable(){ return false;}
    /**
     * @return true if the atom can form an atomic query
     */
    default boolean isSelectable(){ return false;}

    default boolean isRecursive(){ return false;}

    /**
     * @param name variable name
     * @return true if atom contains an occurrence of the variable name
     */
    default boolean containsVar(String name){ return false;}

    /**
     * @return the corresponding pattern
     * */
    PatternAdmin getPattern();

    /**
     * @return the query this atom belongs to
     * */
    Query getParentQuery();

    /**
     * @param q query this atom is supposed to belong to
     */
    void setParentQuery(Query q);

    Map<String, String> getUnifiers(Atomic parentAtom);

    /**
     * change each variable occurrence in the atom (apply unifier [from/to])
     * if capture occurs it is marked with a "capture-><name of the captured occurrence>" name
     * @param from variable name to be changed
     * @param to new variable name
     */
    void unify (String from, String to);

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * if capture occurs it is marked with a "capture-><name of the captured occurrence>" name
     * @param unifiers contain variable mappings to be applied
     */
    void unify(Map<String, String> unifiers);

    String getVarName();

    /**
     * @return all addressable variable names in the atom
     */
    Set<String> getVarNames();
}
