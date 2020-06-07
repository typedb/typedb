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
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * Query state produced by AtomicState
 * It is used to allow rule application to Atomics where only part of the Atomic matches the rule head
 * Answers produced by the partial atomic that is resolved are checked against the full Atomic query
 */
public class PartialAtomicState extends AnswerPropagatorState<ReasonerAtomicQuery>{

    public PartialAtomicState(ReasonerAtomicQuery query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(query, sub, u, parent, subGoals);
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        ReasonerQueryImpl split = getQuery().rewriteAtoms();
        // if the decomposition produced multiple sub-atoms
        if (!split.isAtomic()) {
            return Iterators.singletonIterator(split.resolutionState(getSubstitution(), getUnifier(), this, getVisitedSubGoals()));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        // we take the provided answer, and apply it to the non-rewritten/simplified Atomic query
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
