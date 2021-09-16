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
import java.util.Set;

public abstract class CompoundResolver<RESOLVER extends CompoundResolver<RESOLVER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(CompoundResolver.class);

    final Map<Request.Template, RequestState> requestStates;
    boolean isInitialised;

    protected CompoundResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    protected void nextAnswer(Request fromUpstream, RequestState requestState) {
        if (requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream.trace()), fromUpstream);
        } else if (requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream.trace()), fromUpstream);
        } else if (requestState.downstreamManager().hasNextBlocked()) {
            blockToUpstream(fromUpstream, requestState.downstreamManager().blockers());
        } else {
            failToUpstream(fromUpstream);
        }
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;
        RequestState requestState = requestStates.computeIfAbsent(fromUpstream.template(), this::requestStateCreate);
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;
        RequestState requestState = requestStates.get(fromUpstream.visit().template());
        requestState.downstreamManager().unblock(fromUpstream.cycles());
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Exhausted from {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request.Template toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = upstreamRequest(fromDownstream);
        RequestState requestState = requestStates.get(fromUpstream.visit().template());
        requestState.downstreamManager().remove(toDownstream);
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request.Template cyclingDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = upstreamRequest(fromDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream.visit().template());
        if (requestState.downstreamManager().contains(cyclingDownstream)) {
            requestState.downstreamManager().block(cyclingDownstream, fromDownstream.origins());
        }
        nextAnswer(fromUpstream, requestState);
    }

    abstract RequestState requestStateCreate(Request.Template fromUpstream);

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
