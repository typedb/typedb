/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.framework.Downstream;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class CompoundResolver<RESOLVER extends CompoundResolver<RESOLVER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(CompoundResolver.class);

    final Map<Request.Visit, RequestState> requestStates;
    boolean isInitialised;

    protected CompoundResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    protected void nextAnswer(Request.Visit fromUpstream, RequestState requestState, int iteration) {
        if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream, iteration);
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream, iteration);
        } else if (requestState.downstreamManager().hasNextBlocked()) {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().blockers(), iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream, int iteration) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        RequestState requestState = getOrUpdateRequestState(fromUpstream, iteration);
        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream, int iteration) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;

        RequestState requestState = requestStates.get(fromUpstream.visit());
        requestState.downstreamManager().unblock(fromUpstream.cycles());
        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream.visit(), iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream.visit(), requestState, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        if (isTerminated()) return;

        Downstream toDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        RequestState requestState = requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages back out of the actor model
            failToUpstream(fromUpstream, iteration);
            return;
        }
        requestState.downstreamManager().remove(toDownstream);
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream, int iteration) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Downstream cyclingDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        RequestState requestState = this.requestStates.get(fromUpstream);
        if (requestState.downstreamManager().contains(cyclingDownstream)) {
            requestState.downstreamManager().block(cyclingDownstream, fromDownstream.origins());
        }
        nextAnswer(fromUpstream, requestState, iteration);
    }

    private RequestState getOrUpdateRequestState(Request.Visit fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, requestStateCreate(fromUpstream, iteration));
        } else {
            RequestState requestState = requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState responseProducerNextIter = requestStateReiterate(fromUpstream, requestState, iteration);
                this.requestStates.put(fromUpstream, responseProducerNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    abstract RequestState requestStateCreate(Request.Visit fromUpstream, int iteration);

    abstract RequestState requestStateReiterate(Request.Visit fromUpstream, RequestState priorResponses, int iteration);

    // TODO: Align with the RequestState implementation used across the other resolvers
    static class RequestState {

        private final int iteration;
        private final DownstreamManager downstreamManager;
        private final Set<ConceptMap> deduplicationSet;

        public RequestState(int iteration) {
            this(iteration, new HashSet<>());
        }

        public RequestState(int iteration, Set<ConceptMap> produced) {
            this.iteration = iteration;
            this.downstreamManager = new DownstreamManager();
            this.deduplicationSet = new HashSet<>(produced);
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public int iteration() {
            return iteration;
        }

        public Set<ConceptMap> deduplicationSet() {
            return deduplicationSet;
        }
    }
}
