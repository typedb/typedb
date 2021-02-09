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
 *
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class RuleResolver extends ConjunctionResolver<RuleResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RuleResolver.class);

    private final Rule rule;

    public RuleResolver(Actor<RuleResolver> self, Rule rule, Actor<ResolutionRecorder> resolutionRecorder,
                        ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                        LogicManager logicMgr, Planner planner, boolean explanations) {
        super(self, RuleResolver.class.getSimpleName() + "(rule:" + rule + ")", rule.when(), resolutionRecorder,
              registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
        this.rule = rule;
    }

    @Override
    protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        if (responseProducer.hasUpstreamAnswer()) {
            Partial<?> upstreamAnswer = responseProducer.upstreamAnswers().next();
            responseProducer.recordProduced(upstreamAnswer.conceptMap());
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
        } else {
            if (responseProducer.hasDownstreamProducer()) {
                requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    @Override
    protected ResourceIterator<Partial<?>> toUpstreamAnswers(Request fromUpstream,
                                                             ResourceIterator<ConceptMap> downstreamConceptMaps) {
        return downstreamConceptMaps.flatMap(whenAnswer -> materialisations(fromUpstream.partialAnswer()));
    }

    @Override
    protected Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream) {
        // TODO: we need to record the rest of the iterator in the response producer to pick up later
        // TODO: we should write a test for this case
        ResourceIterator<Partial<?>> materialisations = materialisations(fromDownstream);
        if (materialisations.hasNext()) return Optional.of(materialisations.next());
        else return Optional.empty();
    }

    private ResourceIterator<Partial<?>> materialisations(Partial<?> whenAnswer) {
        ResourceIterator<Map<Identifier, Concept>> materialisations = rule.conclusion()
                .materialise(whenAnswer.conceptMap(), traversalEngine, conceptMgr);
        if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);

        assert whenAnswer.isUnified();
        ResourceIterator<Partial<?>> upstreamAnswers = materialisations
                .map(concepts -> whenAnswer.asUnified().aggregateToUpstream(concepts, self()))
                .filter(Optional::isPresent)
                .map(Optional::get);
        return upstreamAnswers;
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
