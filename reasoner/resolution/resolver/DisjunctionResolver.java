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
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Filtered;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class DisjunctionResolver<T extends DisjunctionResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);
    protected final Actor<ResolutionRecorder> resolutionRecorder;
    protected final List<Actor<ConjunctionResolver.Nested>> downstreamResolvers;
    private final grakn.core.pattern.Disjunction disjunction;
    protected long skipped;
    protected long answered;
    protected boolean isInitialised;
    final Map<Request, ResponseProducer> responseProducers;

    public DisjunctionResolver(Actor<T> self, String name, grakn.core.pattern.Disjunction disjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
        super(self, name, registry, traversalEngine, conceptMgr, explanations);
        this.disjunction = disjunction;
        this.resolutionRecorder = resolutionRecorder;
        this.isInitialised = false;
        this.downstreamResolvers = new ArrayList<>();
        this.responseProducers = new HashMap<>();
    }

    protected abstract void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration);

    protected boolean mustOffset() { return false; }

    protected void offsetOccurred() {}

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) initialiseDownstreamResolvers();

        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == responseProducer.iteration();
            nextAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Partial<?> answer = fromDownstream.answer();
        ConceptMap filteredMap = answer.conceptMap();
        if (!responseProducer.hasProduced(filteredMap)) {
            responseProducer.recordProduced(filteredMap);
            if (mustOffset()) {
                offsetOccurred();
                nextAnswer(fromUpstream, responseProducer, iteration);
                return;
            }
            answerToUpstream(answer, fromUpstream, iteration);
        } else {
            nextAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration failed messages back out of the actor model
            failToUpstream(fromUpstream, iteration);
            return;
        }
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        for (grakn.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
            downstreamResolvers.add(registry.nested(conjunction));
        }
        isInitialised = true;
    }

    private ResponseProducer mayUpdateAndGetResponseProducer(Request fromUpstream, int iteration) {
        if (!responseProducers.containsKey(fromUpstream)) {
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);
            assert responseProducer.iteration() == iteration ||
                    responseProducer.iteration() + 1 == iteration;

            if (responseProducer.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer, iteration);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responseProducers.get(fromUpstream);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
        assert fromUpstream.partialAnswer().isFiltered() || fromUpstream.partialAnswer().isIdentity();
        ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
        assert !downstreamResolvers.isEmpty();
        for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
            Filtered downstream = fromUpstream.partialAnswer()
                    .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver));
            Request request = Request.create(self(), conjunctionResolver, downstream);
            responseProducer.addDownstreamProducer(request);
        }
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        assert newIteration > responseProducerPrevious.iteration();

        ResponseProducer responseProducerNewIter = responseProducerNewIteration(responseProducerPrevious, newIteration);
        for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
            Filtered downstream = fromUpstream.partialAnswer()
                    .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver));
            Request request = Request.create(self(), conjunctionResolver, downstream);
            responseProducerNewIter.addDownstreamProducer(request);
        }
        return responseProducerNewIter;
    }

    abstract ResponseProducer responseProducerNewIteration(ResponseProducer responseProducerPrevious, int newIteration);

    protected Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(Actor<ConjunctionResolver.Nested> conjunctionResolver) {
        // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
        return iterate(conjunctionResolver.state.conjunction.variables()).filter(v -> v.id().isRetrievable())
                .map(v -> v.id().asRetrievable()).toSet();
    }

    public static class Nested extends DisjunctionResolver<Nested> {

        public Nested(Actor<Nested> self, grakn.core.pattern.Disjunction disjunction,
                      Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                      TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
            super(self, Nested.class.getSimpleName() + "(pattern: " + disjunction + ")", disjunction,
                  resolutionRecorder, registry, traversalEngine, conceptMgr, explanations);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                throw GraknException.of(ILLEGAL_STATE);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    failToUpstream(fromUpstream, iteration);
                }
            }
        }

        @Override
        protected ResponseProducer responseProducerNewIteration(ResponseProducer responseProducerPrevious, int newIteration) {
            return responseProducerPrevious.newIteration(Iterators.empty(), newIteration);
        }

    }
}
