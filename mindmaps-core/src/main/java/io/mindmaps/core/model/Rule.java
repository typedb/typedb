/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.model;

import java.util.Collection;

public interface Rule extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param lhs A string representing the left hand side GraQL query.
     * @return The rule itself
     */
    Rule setLHS(String lhs);

    /**
     *
     * @param rhs A string representing the right hand side GraQL query.
     * @return The rule itself
     */
    Rule setRHS(String rhs);

    /**
     *
     * @param expectation
     * @return The rule itself
     */
    Rule setExpectation(boolean expectation);

    /**
     *
     * @param materialise
     * @return The rule itself
     */
    Rule setMaterialise(boolean materialise);
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A string representing the left hand side GraQL query.
     */
    String getLHS();

    /**
     *
     * @return A string representing the right hand side GraQL query.
     */
    String getRHS();

    /**
     *
     * @return
     */
    boolean getExpectation();

    /**
     *
     * @return
     */
    boolean isMaterialise();

    /**
     *
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources();

    //------------------------------------- Edge Handling ----------------------------------
    /**
     *
     * @param type The concept type which this rules applies to.
     * @return The rule itself
     */
    Rule addHypothesis(Type type);
    /**
     *
     * @param type The concept type which is the conclusion of this Rule.
     * @return The rule itself
     */
    Rule addConclusion(Type type);

    /**
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the rule
     */
    Collection<Type> getHypothesisTypes();

    /**
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the rule
     */
    Collection<Type> getConclusionTypes();

    //---- Inherited Methods
    Rule setId(String id);
    Rule setSubject(String subject);
    Rule setValue(String value);
    String getValue();
    RuleType type();
}
