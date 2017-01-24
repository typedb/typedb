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
 * <p>
 *     A rule which defines how implicit knowledge can extracted.
 * </p>
 *
 * <p>
 *     It can behave like any other {@link Instance} but primarily serves as a way of extracting
 *     implicit data from the graph. By defining the LHS (if statment) and RHS (then conclusion) it is possible to
 *     automatically materialise new concepts based on these rules.
 * </p>
 *
 * @author fppt
 *
 */
public interface Rule extends Instance{
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Set the expectation of this Rule to be true or false.
     *
     * @param expectation A boolean parameter representing the expectation setting.
     * @return The Rule itself
     */
    Rule setExpectation(boolean expectation);


    /**
     * Sets whether the Rule should be materialised.
     *
     * @param materialise A boolean representing whether the Rule should be materialised.
     * @return The Rule itself
     */
    Rule setMaterialise(boolean materialise);
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the Left Hand Side of a Graql query.
     *
     * @return A string representing the left hand side Graql query.
     */
    Pattern getLHS();

    /**
     * Retrieves the Right Hand Side of a Graql query.
     *
     * @return A string representing the right hand side Graql query.
     */
    Pattern getRHS();


    /**
     * Returns a boolean representing the expectation setting of this Rule.
     *
     * @return a boolean representing the expectation setting.
     */
    Boolean getExpectation();


    /**
     * Returns whether the Rule is materialised.
     *
     * @return boolean value representing the materialisation setting.
     */
    Boolean isMaterialise();

    //------------------------------------- Edge Handling ----------------------------------
    /**
     * Add a hypothesis of the specified Type to the Rule.
     *
     * @param type The Type which this Rule applies to.
     * @return The Rule itself
     */
    Rule addHypothesis(Type type);
    /**
     *
     * Add a conclusion of the specified Type to the Rule.
     *
     * @param type The Type which is the conclusion of this Rule.
     * @return The Rule itself
     */
    Rule addConclusion(Type type);

    /**
     * Retrieve a set of Types that constitute a part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the Rule
     */
    Collection<Type> getHypothesisTypes();

    /**
     * Retrieve a set of Types that constitue a part of the conclusion of the Rule.
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the Rule
     */
    Collection<Type> getConclusionTypes();

    //---- Inherited Methods
    /**
     *
     * @return The type of this Graql
     */
    RuleType type();
}
