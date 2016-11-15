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

package ai.grakn.concept;

import ai.grakn.graql.Pattern;

import java.util.Collection;

/**
 * A rule represents an instance of a Rule Type which is used to make inferences over the data instances.
 */
public interface Rule extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    //TODO: Fill out details on this method
    /**
     *
     * @param expectation
     * @return The Rule itself
     */
    Rule setExpectation(boolean expectation);

    //TODO: Fill out details on this method
    /**
     *
     * @param materialise
     * @return The Rule itself
     */
    Rule setMaterialise(boolean materialise);
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A string representing the left hand side GraQL query.
     */
    Pattern getLHS();

    /**
     *
     * @return A string representing the right hand side GraQL query.
     */
    Pattern getRHS();

    //TODO: Fill out details on this method
    /**
     *
     * @return
     */
    Boolean getExpectation();

    //TODO: Fill out details on this method
    /**
     *
     * @return
     */
    Boolean isMaterialise();

    //------------------------------------- Edge Handling ----------------------------------
    /**
     *
     * @param type The concept type which this rules applies to.
     * @return The Rule itself
     */
    Rule addHypothesis(Type type);
    /**
     *
     * @param type The concept type which is the conclusion of this Rule.
     * @return The Rule itself
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
    /**
     *
     * @return The type of this rule
     */
    RuleType type();
}
