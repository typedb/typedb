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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class RetrievableResolver extends ResolvableResolver<RetrievableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);
    private final Retrievable retrievable;
    private final Map<Request, ResponseProducer> responseProducers;
    private final ConceptManager conceptMgr;

    public RetrievableResolver(Actor<RetrievableResolver> self, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
        super(self, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.conjunction() + ")",
              registry, traversalEngine, explanations);
        this.retrievable = retrievable;
        this.conceptMgr = conceptMgr;
        this.responseProducers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
        } else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void initialiseDownstreamActors() {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
        Traversal traversal = boundTraversal(retrievable.conjunction().traversal(), fromUpstream.answerBounds().conceptMap());
        ResourceIterator<ConceptMap> traversalProducer = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        return new ResponseProducer(traversalProducer, iteration);
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        assert newIteration > responseProducerPrevious.iteration();
        Traversal traversal = boundTraversal(retrievable.conjunction().traversal(), fromUpstream.answerBounds().conceptMap());
        ResourceIterator<ConceptMap> traversalProducer = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        return responseProducerPrevious.newIteration(traversalProducer, newIteration);
    }

    private ResponseProducer mayUpdateAndGetResponseProducer(Request fromUpstream, int iteration) {
        if (!responseProducers.containsKey(fromUpstream)) {
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);
            assert iteration <= responseProducer.iteration() + 1;

            if (responseProducer.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer, iteration);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responseProducers.get(fromUpstream);
    }

    private void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            AnswerState.UpstreamVars.Derived derivedAnswer = fromUpstream.answerBounds().asMapped().mapToUpstream(conceptMap);
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(derivedAnswer, retrievable.conjunction().toString(),
                                                               ResolutionAnswer.Derivation.EMPTY, self(), false);
                respondToUpstream(Answer.create(fromUpstream, answer), iteration);
                return;
            }
        }
        respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
    }

}
