/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

// note: in the future, we may introduce query rewriting here
public class ConditionResolver extends ConjunctionResolver<ConditionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionResolver.class);

    private final Rule.Condition condition;
    private final Set<Identifier.Variable.Retrievable> missingBounds;

    public ConditionResolver(Driver<ConditionResolver> driver, Rule.Condition condition, Driver<ResolutionRecorder> resolutionRecorder,
                             ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                             LogicManager logicMgr, Planner planner, boolean resolutionTracing) {
        super(driver, ConditionResolver.class.getCanonicalName() + "(rule:" + condition.rule().getLabel() + ")",
              resolutionRecorder, registry, traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing);
        this.condition = condition;
        this.missingBounds = missingBounds(conjunction());
    }

    @Override
    public Conjunction conjunction() {
        return condition.rule().when();
    }

    @Override
    Set<Identifier.Variable.Retrievable> missingBounds() {
        return missingBounds;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return condition.concludablesTriggeringRules(conceptMgr, logicMgr);
    }

    @Override
    protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        if (requestState.hasDownstreamProducer()) {
            requestFromDownstream(requestState.nextDownstreamProducer(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    protected Optional<AnswerState> toUpstreamAnswer(AnswerState.Partial<?> fromDownstream) {
        return Optional.of(fromDownstream.asFiltered().toUpstream());
    }


    @Override
    ConjunctionResolver.RequestState requestStateNew(int iteration) {
        return new ConjunctionResolver.RequestState(iteration);
    }

    @Override
    ConjunctionResolver.RequestState requestStateNew(int iteration, boolean singleAnswerRequired) {
        return new RequestState(iteration, singleAnswerRequired);
    }
    }

    @Override
    ConjunctionResolver.RequestState requestStateForIteration(RequestState requestStatePrior, int iteration, boolean singleAnswerRequired) {
        return new RequestState(iteration, singleAnswerRequired);
    }

    @Override
    public String toString() {
        return name() + ": " + condition.rule().when();
    }

}
