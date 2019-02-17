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

package grakn.core.graql.answer;

import grakn.core.graql.internal.reasoner.query.ReasonerQuery;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Base class for explanation classes.
 */
public interface Explanation {

    /**
     * produce a new explanation with a provided parent answer
     *
     * @param ans parent answer
     * @return new explanation with dependent answers
     */
    @CheckReturnValue
    Explanation childOf(ConceptMap ans);

    /**
     * @return query associated with this explanation
     */
    @Nullable
    @CheckReturnValue
    ReasonerQuery getQuery();

    /**
     * produce a new explanation with provided query set
     *
     * @param q query this explanation should be associated with
     * @return explanation with provided query
     */
    @CheckReturnValue
    Explanation setQuery(ReasonerQuery q);

    /**
     * @return answers this explanation is dependent on
     */
    @CheckReturnValue
    List<ConceptMap> getAnswers();

    /**
     * @return set of answers corresponding to the explicit path
     */
    @CheckReturnValue
    Set<ConceptMap> explicit();

    /**
     * @return set of all answers taking part in the derivation of this answer
     */
    @CheckReturnValue
    Set<ConceptMap> deductions();

    /**
     * @return true if this explanation explains the answer on the basis of database lookup
     */
    @CheckReturnValue
    boolean isLookupExplanation();

    /**
     * @return true if this explanation explains the answer on the basis of rule application
     */
    @CheckReturnValue
    boolean isRuleExplanation();

    /**
     * @return true if this explanation explains an intermediate answer being a product of a join operation
     */
    @CheckReturnValue
    boolean isJoinExplanation();

    /**
     * @return true if this is an empty explanation (explanation wasn't recorded)
     */
    @CheckReturnValue
    boolean isEmpty();
}
