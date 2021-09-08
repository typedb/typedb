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

    protected void nextAnswer(Request.Visit fromUpstream, RequestState requestState) {
        if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream);
        } else if (requestState.downstreamManager().hasNextBlocked()) {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().blockers());
        } else {
            failToUpstream(fromUpstream);
        }
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;
        RequestState requestState = requestStates.computeIfAbsent(fromUpstream, this::requestStateCreate);
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;

        RequestState requestState = requestStates.get(fromUpstream.visit());
        requestState.downstreamManager().unblock(fromUpstream.cycles());
        nextAnswer(fromUpstream.visit(), requestState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Exhausted from {}", name(), fromDownstream);
        if (isTerminated()) return;

        Downstream toDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        RequestState requestState = requestStates.get(fromUpstream);
        requestState.downstreamManager().remove(toDownstream);
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Downstream cyclingDownstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        RequestState requestState = this.requestStates.get(fromUpstream);
        if (requestState.downstreamManager().contains(cyclingDownstream)) {
            requestState.downstreamManager().block(cyclingDownstream, fromDownstream.origins());
        }
        nextAnswer(fromUpstream, requestState);
    }

    abstract RequestState requestStateCreate(Request.Visit fromUpstream);

    // TODO: Align with the RequestState implementation used across the other resolvers
    static class RequestState {

        private final DownstreamManager downstreamManager;
        private final Set<ConceptMap> deduplicationSet;

        public RequestState() {
            this(new HashSet<>());
        }

        public RequestState(Set<ConceptMap> produced) {
            this.downstreamManager = new DownstreamManager();
            this.deduplicationSet = new HashSet<>(produced);
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public Set<ConceptMap> deduplicationSet() {
            return deduplicationSet;
        }
    }
}
