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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.admin;

import com.google.common.collect.ImmutableList;
import javax.annotation.CheckReturnValue;

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

    /**
     * produce a new explanation with provided query set
     * @param q query this explanation should be associated with
     * @return explanation with provided query
     */
    @CheckReturnValue
    AnswerExplanation setQuery(ReasonerQuery q);

    /**
     * produce a new explanation with a provided parent answer
     * @param ans parent answer
     * @return new explanation with dependent answers
     */
    @CheckReturnValue
    AnswerExplanation childOf(Answer ans);

    /**
     * @return query associated with this explanation
     */
    @CheckReturnValue
    ReasonerQuery getQuery();

    /**
     * @return answers this explanation is dependent on
     */
    @CheckReturnValue
    ImmutableList<Answer> getAnswers();

    /**
     *
     * @return true if this explanation explains the answer on the basis of database lookup
     */
    @CheckReturnValue
    boolean isLookupExplanation();

    /**
     *
     * @return true if this explanation explains the answer on the basis of rule application
     */
    @CheckReturnValue
    boolean isRuleExplanation();

    /**
     *
     * @return true if this explanation explains an intermediate answer being a product of a join operation
     */
    @CheckReturnValue
    boolean isJoinExplanation();

    /**
     *
     * @return true if this is an empty explanation (explanation wasn't recorded)
     */
    @CheckReturnValue
    boolean isEmpty();
}
