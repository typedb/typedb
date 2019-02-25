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

package grakn.core.graql.reasoner.atom;

import grakn.core.concept.type.Rule;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.HashSet;
import java.util.Set;

/**
 * Basic interface for logical atoms used in reasoning.
 */
public interface Atomic {

    @CheckReturnValue
    Atomic copy(ReasonerQuery parent);

    @CheckReturnValue
    Atomic neqPositive();

    /**
     * @return variable name of this atomic
     */
    @CheckReturnValue
    Variable getVarName();

    /**
     * @return the corresponding base pattern
     * */
    @CheckReturnValue
    Statement getPattern();

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
     */
    @CheckReturnValue
    default boolean isResource(){ return false;}

    /**
     * @return if atom contains properties considering only explicit type hierarchies
     */
    @CheckReturnValue
    default boolean isDirect(){ return false;}

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
     * Determines whether the subsumption relation between this (A) and provided atom (B) holds,
     * i. e. determines if:
     *
     * A <= B
     *
     * is true meaning that B is more general than A and the respective answer sets meet:
     *
     * answers(A) subsetOf answers(B)
     *
     * i. e. the set of answers of A is a subset of the set of answers of B
     *
     * @param atom to compare with
     * @return true if this atom subsumes the provided atom
     */
    @CheckReturnValue
    boolean subsumes(Atomic atom);

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
    Set<Variable> getVarNames();

    /**
     * infers types (type, role types) for the atom if applicable/possible
     * @return either this atom if nothing could be inferred or a fresh atom with inferred types
     */
    @CheckReturnValue
    Atomic inferTypes();

}
