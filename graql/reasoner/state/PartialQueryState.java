/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.state;

import com.google.common.collect.Iterators;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * Query state produced by atomic or conjunctive queries.
 * Used for queries that require role-player decomposition - only part of the atom matches the rule head.
 * As the decomposition may alter query semantics, answers produced by the partial state are checked against the original full query.
 */
public class PartialQueryState extends AnswerPropagatorState<ResolvableQuery>{

    public PartialQueryState(ResolvableQuery query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(query, sub, u, parent, subGoals);
    }

    @Override
    public String toString(){
        return super.toString() + "rewrite:\n" + getQuery().rewrite() + "\n";
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        ResolvableQuery split = getQuery().rewrite();
        // if the decomposition is atomic then it is trivial
        if (split.isAtomic()) return Collections.emptyIterator();

        return Iterators.singletonIterator(split.resolutionState(getSubstitution(), getUnifier(), this, getVisitedSubGoals()));
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        //as the query rewrite can possibly alter the semantics of the query, we take the combined answer, and execute it against to the original query
        ConceptMap answer = consumeAnswer(state);
        Optional<ConceptMap> satisfiesFullQuery = getQuery().withSubstitution(answer).resolve(false).findFirst();
        if (satisfiesFullQuery.isPresent()) {
            return new AnswerState(answer, state.getUnifier(), getParentState());
        } else {
            return null;
        }
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        // rewrite the pattern that is present
        ConceptMap answer = state.getSubstitution();
        return answer.withPattern(getQuery().getPattern());
    }
}
