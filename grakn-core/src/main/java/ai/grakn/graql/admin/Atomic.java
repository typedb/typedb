/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.admin;

import ai.grakn.concept.Rule;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;

import javax.annotation.CheckReturnValue;
import java.util.HashSet;
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
    Atomic copy(ReasonerQuery parent);

    /**
     * @return variable name of this atomic
     */
    @CheckReturnValue
    Var getVarName();

    /**
     * @return the corresponding base pattern
     * */
    @CheckReturnValue
    VarPattern getPattern();

    /**
     * @return the {@link ReasonerQuery} this atomic belongs to
     */
    ReasonerQuery getParentQuery();

    /**
     * validate wrt transaction the atomic is defined in
     */
    void checkValid();

    /**
     * @return true if the atomic corresponds to a atom
     * */
    @CheckReturnValue
    default boolean isAtom(){ return false;}

    /**
     * @return true if the atomic corresponds to a type atom
     * */
    @CheckReturnValue
    default boolean isType(){ return false;}

    /**
     * @return true if the atomic corresponds to a relation atom
     * */
    @CheckReturnValue
    default boolean isRelation(){return false;}

    /**
     * @return true if the atomic corresponds to a resource atom
     * */
    default boolean isResource(){ return false;}

    /**
     * @return true if obj alpha-equivalent
     */
    @CheckReturnValue
    boolean isAlphaEquivalent(Object obj);

    /**
     * @return true if obj structurally equivalent
     */
    @CheckReturnValue
    boolean isStructurallyEquivalent(Object obj);

    /**
     * @return true if obj compatible
     */
    @CheckReturnValue
    default boolean isCompatibleWith(Object obj){return isAlphaEquivalent(obj);}

    /**
     * @return alpha-equivalence hash code
     */
    @CheckReturnValue
    int alphaEquivalenceHashCode();

    /**
     * @return structural-equivalence hash code
     */
    @CheckReturnValue
    int structuralEquivalenceHashCode();

    /**
     * @return true if the atomic can form an atomic query
     */
    @CheckReturnValue
    default boolean isSelectable(){ return false;}

    /**
     * @return true if the atomic can constitute the head of a rule
     */
    @CheckReturnValue
    Set<String> validateAsRuleHead(Rule rule);

    /**
     * @return error messages indicating ontological inconsistencies of this atomic
     */
    @CheckReturnValue
    default Set<String> validateOntologically(){ return new HashSet<>();}

    /**
     * @return the base pattern combined with possible predicate patterns
     */
    @CheckReturnValue
    Pattern getCombinedPattern();

    /**
     * @return all addressable variable names in this atomic
     */
    @CheckReturnValue
    Set<Var> getVarNames();

    /**
     * infers types (type, role types) for the atom if applicable/possible
     * @return either this atom if nothing could be inferred or a fresh atom with inferred types
     */
    @CheckReturnValue
    Atomic inferTypes();

}
