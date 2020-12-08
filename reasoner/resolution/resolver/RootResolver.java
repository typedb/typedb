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
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.Unifier;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class RootResolver extends ConjunctionResolver<RootResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RootResolver.class);

    private final Consumer<ResolutionAnswer> onAnswer;
    private final Runnable onExhausted;

    public RootResolver(Actor<RootResolver> self, Conjunction conjunction,
                        Long traversalSize, Consumer<ResolutionAnswer> onAnswer, Runnable onExhausted) {
        super(self, RootResolver.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, traversalSize);
        this.onAnswer = onAnswer;
        this.onExhausted = onExhausted;
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        // TODO Refactor to remove duplication against RuleResolver
        Actor<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
        ConceptMap conceptMap = fromDownstream.sourceRequest().partialConceptMap().merge(fromDownstream.answer().conceptMap()).unUnify();

        ResolutionAnswer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.trace("{}: has answer: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                ResolutionAnswer answer = new ResolutionAnswer(conceptMap, conjunction.toString(), derivation, self());
                return Either.second(rootAnswerProduced(fromUpstream, answer));
            } else {
                return produceMessage(fromUpstream, responseProducer);
            }
        } else {
            Pair<Actor<ConcludableResolver>, Unifier> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream.first()),
                                                    nextPlannedDownstream.second().unify(conceptMap), derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return produceMessage(fromUpstream, responseProducer);
    }

    Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: traversal answer: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(conceptMap, conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self());
                return Either.second(rootAnswerProduced(fromUpstream, answer));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            onExhausted.run();
            return Either.second(new Response.RootResponse(fromUpstream));
        }
    }

    private Response.RootResponse rootAnswerProduced(Request fromUpstream, final ResolutionAnswer answer) {
        LOG.debug("Responding RootResponse and Recording root answer execution tree for: {}", answer.conceptMap());
        resolutionRecorder.tell(state -> state.record(answer));
        onAnswer.accept(answer);
        return new Response.RootResponse(fromUpstream);
    }
}
