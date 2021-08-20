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
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class CompoundResolver<RESOLVER extends CompoundResolver<RESOLVER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(CompoundResolver.class);

    final Map<Request, RequestState> requestStates;
    boolean isInitialised;

    protected CompoundResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        if (requestState.downstreamManager().hasDownstream()) {
            requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
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
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages back out of the actor model
            failToUpstream(fromUpstream, iteration);
            return;
        }
        requestState.downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream, int iteration) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request blockedDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(blockedDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);
        fromDownstream.blockers().forEach(blocker -> requestState.downstreamManager().block(blockedDownstream, blocker));
        Optional<Request> unblocked = requestState.downstreamManager().nextUnblockedDownstream();
        if (unblocked.isPresent()) {
            requestFromDownstream(unblocked.get(), fromUpstream, iteration);
        } else {
            blockToUpstream(fromUpstream, requestState.downstreamManager().blockers(), iteration);
        }
    }

    private RequestState getOrUpdateRequestState(Request fromUpstream, int iteration) {
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

    abstract RequestState requestStateCreate(Request fromUpstream, int iteration);

    abstract RequestState requestStateReiterate(Request fromUpstream, RequestState priorResponses, int iteration);

    // TODO: Align with the RequestState implementation used across the other resolvers
    class RequestState {

        private final int iteration;
        private final DownstreamManager.Blockable downstreamManager;
        private final Set<ConceptMap> deduplicationSet;

        public RequestState(int iteration) {
            this(iteration, new HashSet<>());
        }

        public RequestState(int iteration, Set<ConceptMap> produced) {
            this.iteration = iteration;
            this.downstreamManager = new DownstreamManager.Blockable();
            this.deduplicationSet = new HashSet<>(produced);
        }

        public DownstreamManager.Blockable downstreamManager() {
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
