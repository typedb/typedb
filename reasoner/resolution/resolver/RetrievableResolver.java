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

import grakn.core.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static grakn.common.collection.Collections.map;

public class RetrievableResolver extends ResolvableResolver<RetrievableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);
    private final Retrievable retrievable;
    private final ResolutionRecorder resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private ResponseProducer responseProducer;

    public RetrievableResolver(Actor<RetrievableResolver> self, Retrievable retrievable,
                               ResolutionRecorder resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, boolean explanations) {
        super(self, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable + ")", registry, traversalEngine, explanations);
        this.retrievable = retrievable;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
    }


    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        responseProducer = responseProducerCreate(fromUpstream, iteration);
        mayReiterateResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
        } else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        // No downstream actors, so do nothing
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        // No downstream actors, so do nothing
    }

    @Override
    protected void initialiseDownstreamActors() {
        // No downstream actors, so do nothing
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(retrievable.conjunction(), new ConceptMap());
        return new ResponseProducer(traversal, iteration);
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        assert newIteration > responseProducerPrevious.iteration();
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(retrievable.conjunction(), new ConceptMap());
        return responseProducerPrevious.newIteration(traversal, newIteration);
    }

    private void tryAnswer(Request fromUpstream, int iteration) {
        // TODO This method WIP



        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                assert fromUpstream.answerBounds().isRoot();
                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.answerBounds().asRoot().aggregateToUpstream(conceptMap),
                                                               retrievable.conjunction().toString(), ResolutionAnswer.Derivation.EMPTY, self(), false);
                submitAnswer(answer);
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
        } else {
            onExhausted.accept(iteration);
        }
    }

    private void mayReiterateResponseProducer(Request fromUpstream, int iteration) {
        // TODO This method WIP
        if (responseProducer.iteration() + 1 == iteration) {
            responseProducer = responseProducerReiterate(fromUpstream, responseProducer, iteration);
        }
    }

    private void submitAnswer(ResolutionAnswer answer) {
        // TODO This method WIP
        LOG.debug("Submitting root answer: {}", answer.derived());
        resolutionRecorder.tell(state -> state.record(answer));
        onAnswer.accept(answer);
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
    }

}
