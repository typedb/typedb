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

import com.google.common.collect.Iterables;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.graql.reasoner.explanation.JoinExplanation;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Query state corresponding to a an intermediate state obtained from decomposing a conjunctive query (ReasonerQueryImpl) in the resolution tree.
 */
public class JoinState extends AnswerPropagatorState<ReasonerQueryImpl> {

    private final LinkedList<ReasonerQueryImpl> subQueries;

    public JoinState(List<ReasonerQueryImpl> qs,
                     ConceptMap sub,
                     Unifier u,
                     AnswerPropagatorState parent,
                     Set<ReasonerAtomicQuery> subGoals) {
        super(Iterables.getFirst(qs, null), sub, u, parent, subGoals);
        this.subQueries = new LinkedList<>(qs);
        subQueries.removeFirst();
    }

    @Override
    protected Iterator<ResolutionState> generateChildStateIterator() {
        //NB: we need lazy resolutionState initialisation here, otherwise they are marked as visited before visit happens
        return getQuery().expandedStates(getSubstitution(), getUnifier(), this, getVisitedSubGoals()).iterator();
    }


    @Override
    public String toString(){
        return super.toString() +  "\n" +
                getSubstitution() + "\n" +
                getQuery() + "\n" +
                subQueries.stream().map(ReasonerQueryImpl::toString).collect(Collectors.joining("\n")) + "\n";
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap accumulatedAnswer = getSubstitution();
        // we need to pass ID substitutions whenever we set the pattern from raw query
        ConceptMap toMerge = state.getSubstitution().withPattern(getQuery().withSubstitution(state.getSubstitution()).getPattern());
        ConceptMap merged = AnswerUtil.joinAnswers(accumulatedAnswer, toMerge);
        ConceptMap answer = new ConceptMap(
                merged.map(),
                mergeExplanations(accumulatedAnswer, toMerge),
                merged.getPattern());

        if (answer.isEmpty()) return null;
        if (subQueries.isEmpty()) return new AnswerState(answer, getUnifier(), getParentState());
        return new JoinState(subQueries, answer, getUnifier(), getParentState(), getVisitedSubGoals());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }

    private static Explanation mergeExplanations(ConceptMap base, ConceptMap toMerge) {
        if (toMerge.isEmpty()) return base.explanation();
        if (base.isEmpty()) return toMerge.explanation();

        List<ConceptMap> partialAnswers = new ArrayList<>();
        if (base.explanation().isJoinExplanation()) partialAnswers.addAll(base.explanation().getAnswers());
        else partialAnswers.add(base);
        if (toMerge.explanation().isJoinExplanation()) partialAnswers.addAll(toMerge.explanation().getAnswers());
        else partialAnswers.add(toMerge);
        return new JoinExplanation(partialAnswers);
    }

}
