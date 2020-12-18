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

package grakn.core.reasoner.resolution.resolver;

import grakn.common.collection.Either;
import grakn.common.collection.Pair;
import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.logic.transformer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

public class RootResolver extends ConjunctionResolver<RootResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RootResolver.class);

    private final Consumer<ResolutionAnswer> onAnswer;
    private final Runnable onExhausted;

    public RootResolver(Actor<RootResolver> self, Conjunction conjunction, Consumer<ResolutionAnswer> onAnswer,
                        Runnable onExhausted) {
        super(self, RootResolver.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, ConjunctionConcludable.create(conjunction));
        this.onAnswer = onAnswer;
        this.onExhausted = onExhausted;
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream,
                                                      ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
        ConceptMap conceptMap = fromDownstream.answer().derived().map();

        ResolutionAnswer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.trace("{}: has produced: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partial().aggregateWith(conceptMap).asDerived(),
                                                               conjunction.toString(), derivation, self());
                return Either.second(createResponse(fromUpstream, answer));
            } else {
                return messageToSend(fromUpstream, responseProducer);
            }
        } else {
            Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream.first()),
                                                    AnswerState.UpstreamVars.Partial.of(conceptMap).toDownstreamVars(
                                                            Mapping.of(nextPlannedDownstream.second())),
                                                    derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    Either<Request, Response> messageToSend(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: traversal answer: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partial().aggregateWith(conceptMap).asDerived(),
                                                               conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self());
                return Either.second(createResponse(fromUpstream, answer));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            onExhausted.run();
            return Either.second(new Response.RootResponse(fromUpstream));
        }
    }

    Response.RootResponse createResponse(Request fromUpstream, final ResolutionAnswer answer) {
        LOG.debug("Responding RootResponse and Recording root answer execution tree for: {}", answer.derived());
        resolutionRecorder.tell(state -> state.record(answer));
        onAnswer.accept(answer);
        return new Response.RootResponse(fromUpstream);
    }
}
