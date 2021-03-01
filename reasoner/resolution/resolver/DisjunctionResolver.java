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
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

public abstract class DisjunctionResolver<RESOLVER extends DisjunctionResolver<RESOLVER>> extends CompoundResolver<RESOLVER, DisjunctionResolver.RequestState> {

    private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);

    protected final List<Actor<ConjunctionResolver.Nested>> downstreamResolvers;
    private final grakn.core.pattern.Disjunction disjunction;
    protected long skipped;
    protected long answered;

    public DisjunctionResolver(Actor<RESOLVER> self, String name, grakn.core.pattern.Disjunction disjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
        super(self, name, registry, traversalEngine, conceptMgr, explanations, resolutionRecorder);
        this.disjunction = disjunction;
        this.downstreamResolvers = new ArrayList<>();
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = requestStates.get(fromUpstream);

        AnswerState answer = toUpstreamAnswer(fromDownstream.answer());
        boolean acceptedAnswer = tryAcceptUpstreamAnswer(answer, fromUpstream, iteration);
        if (!acceptedAnswer) nextAnswer(fromUpstream, requestState, iteration);
    }

    protected abstract boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration);

    protected abstract AnswerState toUpstreamAnswer(Partial<?> answer);

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        for (grakn.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
            try {
                downstreamResolvers.add(registry.nested(conjunction));
            } catch (GraknException e) {
                terminate(e);
                return;
            }
        }
        if (!isTerminated()) isInitialised = true;
    }

    @Override
    protected RequestState requestStateCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new RequestState for request: {}", name(), fromUpstream);
        assert fromUpstream.partialAnswer().isFiltered() || fromUpstream.partialAnswer().isIdentity();
        RequestState requestState = new RequestState(iteration);
        for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
            AnswerState.Partial.Filtered downstream = fromUpstream.partialAnswer()
                    .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver), conjunctionResolver);
            Request request = Request.create(self(), conjunctionResolver, downstream);
            requestState.addDownstreamProducer(request);
        }
        return requestState;
    }

    @Override
    protected RequestState requestStateReiterate(Request fromUpstream, RequestState requestStatePrior,
                                                 int newIteration) {
        LOG.debug("{}: Updating RequestState for iteration '{}'", name(), newIteration);

        assert newIteration > requestStatePrior.iteration();

        RequestState requestStateNextIteration = requestStateForIteration(requestStatePrior, newIteration);
        for (Actor<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers) {
            AnswerState.Partial.Filtered downstream = fromUpstream.partialAnswer()
                    .filterToDownstream(conjunctionRetrievedIds(conjunctionResolver), conjunctionResolver);
            Request request = Request.create(self(), conjunctionResolver, downstream);
            requestStateNextIteration.addDownstreamProducer(request);
        }
        return requestStateNextIteration;
    }

    abstract RequestState requestStateForIteration(RequestState requestStatePrior, int newIteration);

    protected Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(Actor<ConjunctionResolver.Nested> conjunctionResolver) {
        // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
        return iterate(conjunctionResolver.state().conjunction.variables()).filter(v -> v.id().isRetrievable())
                .map(v -> v.id().asRetrievable()).toSet();
    }

    static class RequestState extends CompoundResolver.RequestState {

        private final Set<ConceptMap> produced;

        public RequestState(int iteration) {
            this(iteration, new HashSet<>());
        }

        public RequestState(int iteration, Set<ConceptMap> produced) {
            super(iteration);
            this.produced = produced;
        }

        public void recordProduced(ConceptMap conceptMap) {
            produced.add(conceptMap);
        }

        public boolean hasProduced(ConceptMap conceptMap) {
            return produced.contains(conceptMap);
        }

        public Set<ConceptMap> produced() {
            return produced;
        }
    }

    public static class Nested extends DisjunctionResolver<Nested> {

        public Nested(Actor<Nested> self, grakn.core.pattern.Disjunction disjunction,
                      Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                      TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
            super(self, Nested.class.getSimpleName() + "(pattern: " + disjunction + ")", disjunction,
                  resolutionRecorder, registry, traversalEngine, conceptMgr, explanations);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, DisjunctionResolver.RequestState requestState, int iteration) {
            if (requestState.hasDownstreamProducer()) {
                requestFromDownstream(requestState.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
            return true;
        }

        @Override
        protected AnswerState toUpstreamAnswer(Partial<?> answer) {
            assert answer.isFiltered();
            return answer.asFiltered().toUpstream();
        }

        @Override
        protected DisjunctionResolver.RequestState requestStateForIteration(DisjunctionResolver.RequestState requestStatePrior, int newIteration) {
            return new DisjunctionResolver.RequestState(newIteration);
        }

    }
}
