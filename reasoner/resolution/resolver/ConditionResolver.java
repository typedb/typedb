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
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

// note: in the future, we may introduce query rewriting here
public class ConditionResolver extends ConjunctionResolver<ConditionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionResolver.class);

    private final Rule rule;

    public ConditionResolver(Driver<ConditionResolver> driver, Rule rule, Driver<ResolutionRecorder> resolutionRecorder,
                             ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                             LogicManager logicMgr, Planner planner, boolean resolutionTracing) {
        super(driver, ConditionResolver.class.getCanonicalName() + "(rule:" + rule.getLabel() + ")", rule.when(),
              resolutionRecorder, registry, traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing);
        this.rule = rule;
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
    ConjunctionResolver.RequestState requestStateForIteration(ConjunctionResolver.RequestState requestStatePrior, int iteration) {
        return new ConjunctionResolver.RequestState(iteration);
    }

    @Override
    public String toString() {
        return name() + ": " + rule.when();
    }

}
