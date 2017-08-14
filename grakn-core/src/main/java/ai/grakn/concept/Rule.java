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

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * <p>
 *     A rule which defines how implicit knowledge can extracted.
 * </p>
 *
 * <p>
 *     It can behave like any other {@link Thing} but primarily serves as a way of extracting
 *     implicit data from the graph. By defining the LHS (if statment) and RHS (then conclusion) it is possible to
 *     automatically materialise new concepts based on these rules.
 * </p>
 *
 * @author fppt
 *
 */
public interface Rule extends Thing {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Creates a relation from this instance to the provided resource.
     *
     * @param resource The resource to which a relationship is created
     * @return The instance itself
     */
    @Override
    Rule resource(Resource resource);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the Left Hand Side of the rule.
     * When this query is satisfied the "then" part of the rule is executed.
     *
     * @return A string representing the left hand side Graql query.
     */
    @CheckReturnValue
    Pattern getWhen();

    /**
     * Retrieves the Right Hand Side of the rule.
     * This query is executed when the "when" part of the rule is satisfied
     *
     * @return A string representing the right hand side Graql query.
     */
    @CheckReturnValue
    Pattern getThen();

    //------------------------------------- Edge Handling ----------------------------------
    /**
     * Retrieve a set of Types that constitute a part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the Rule
     */
    @CheckReturnValue
    Stream<Type> getHypothesisTypes();

    /**
     * Retrieve a set of Types that constitue a part of the conclusion of the Rule.
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the Rule
     */
    @CheckReturnValue
    Stream<Type> getConclusionTypes();

    //---- Inherited Methods
    /**
     *
     * @return The type of this Graql
     */
    @Override
    RuleType type();


    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Rule asRule(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRule(){
        return true;
    }
}
