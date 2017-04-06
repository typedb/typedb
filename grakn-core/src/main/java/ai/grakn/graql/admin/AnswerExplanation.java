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

import java.util.Set;

/**
 *
 * <p>
 * Base class for explanation classes.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface AnswerExplanation {

    AnswerExplanation copy();

    /**
     * @return query associated with this explanation
     */
    ReasonerQuery getQuery();

    /**
     * @param q query this explanation should be associated with
     * @return explanation with provided query
     */
    AnswerExplanation setQuery(ReasonerQuery q);

    /**
     * @param a answer this explanation is dependent on
     * @return true if added successfully
     */
    boolean addAnswer(Answer a);

    /**
     * @return answers this explanation is dependent on
     */
    Set<Answer> getAnswers();

    /**
     * @param a2 explanation to be merged with
     * @return merged explanation
     */
    AnswerExplanation merge(AnswerExplanation a2);

    /**
     *
     * @return true if this explanation explains the answer on the basis of database lookup
     */
    boolean isLookupExplanation();

    /**
     *
     * @return true if this explanation explains the answer on the basis of rule application
     */
    boolean isRuleExplanation();

    /**
     *
     * @return true if this explanation explains an intermediate answer being a product of a join operation
     */
    boolean isJoinExplanation();

    /**
     *
     * @return true if this is an empty explanation (explanation wasn't recorded)
     */
    boolean isEmpty();
}
