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
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
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

    final Map<Compound<?, ?>, ResolutionState> resolutionStates;
    boolean isInitialised;

    protected CompoundResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.resolutionStates = new HashMap<>();
        this.isInitialised = false;
    }

    protected void sendNextMessage(Request fromUpstream, ResolutionState resolutionState) {
        if (resolutionState.explorationManager().hasNextVisit()) {
            visitDownstream(resolutionState.explorationManager().nextVisit(fromUpstream.trace()), fromUpstream);
        } else if (resolutionState.explorationManager().hasNextRevisit()) {
            revisitDownstream(resolutionState.explorationManager().nextRevisit(fromUpstream.trace()), fromUpstream);
        } else if (resolutionState.explorationManager().hasNextBlocked()) {
            blockToUpstream(fromUpstream, resolutionState.explorationManager().blockingCycles());
        } else {
            failToUpstream(fromUpstream);
        }
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;
        ResolutionState resolutionState = resolutionStates.computeIfAbsent(fromUpstream.partialAnswer().asCompound(),
                                                                           this::resolutionStateCreate);
        sendNextMessage(fromUpstream, resolutionState);
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;
        ResolutionState resolutionState = resolutionStates.get(fromUpstream.visit().partialAnswer().asCompound());
        resolutionState.explorationManager().revisit(fromUpstream.cycles());
        sendNextMessage(fromUpstream, resolutionState);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Exhausted from {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request fromUpstream = upstreamRequest(fromDownstream);
        ResolutionState resolutionState = resolutionStates.get(fromUpstream.visit().partialAnswer().asCompound());
        resolutionState.explorationManager().remove(fromDownstream.sourceRequest().visit().factory());
        sendNextMessage(fromUpstream, resolutionState);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request fromUpstream = upstreamRequest(fromDownstream);
        ResolutionState resolutionState = this.resolutionStates.get(fromUpstream.visit().partialAnswer().asCompound());
        Request.Factory blockingDownstream = fromDownstream.sourceRequest().visit().factory();
        if (resolutionState.explorationManager().contains(blockingDownstream)) {
            resolutionState.explorationManager().block(blockingDownstream, fromDownstream.cycles());
        }
        sendNextMessage(fromUpstream, resolutionState);
    }

    abstract ResolutionState resolutionStateCreate(Compound<?, ?> fromUpstream);

    // TODO: Align with the ResolutionState implementation used across the other resolvers
    static class ResolutionState {

        private final ExplorationManager explorationManager;
        private final Set<ConceptMap> deduplicationSet;

        public ResolutionState() {
            this(new HashSet<>());
        }

        public ResolutionState(Set<ConceptMap> produced) {
            this.explorationManager = new ExplorationManager();
            this.deduplicationSet = new HashSet<>(produced);
        }

        public ExplorationManager explorationManager() {
            return explorationManager;
        }

        public Set<ConceptMap> deduplicationSet() {
            return deduplicationSet;
        }

    }
}
