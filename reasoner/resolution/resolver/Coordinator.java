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

import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class Coordinator<
        RESOLVER extends Coordinator<RESOLVER, WORKER>,
        WORKER extends Resolver<WORKER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
    private final Map<Driver<? extends Resolver<?>>, SubsumptionTracker> subsumptionTrackers;
    private final Map<Driver<? extends Resolver<?>>, Map<Request, Request>> requestMapByRoot;
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, Driver<WORKER>>> workersByRoot; // TODO: We would like these not to be by root. They need to be, for now, for reiteration purposes.
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, AnswerCache<?, ConceptMap>>> cacheRegistersByRoot;
    protected final Map<Driver<? extends Resolver<?>>, Integer> iterationByRoot;
    protected boolean isInitialised;

    public Coordinator(Driver<RESOLVER> driver, String name, ResolverRegistry registry, TraversalEngine traversalEngine,
                       ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, name, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.isInitialised = false;
        this.workersByRoot = new HashMap<>();
        this.cacheRegistersByRoot = new HashMap<>();
        this.iterationByRoot = new HashMap<>();
        this.requestMapByRoot = new HashMap<>();
        this.subsumptionTrackers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        iterationByRoot.putIfAbsent(root, iteration);
        if (iteration > iterationByRoot.get(root)) {
            prepareNextIteration(root, iteration);
        }
        if (iteration < iterationByRoot.get(root)) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            ConceptMap bounds = fromUpstream.partialAnswer().conceptMap();
            Driver<WORKER> worker = getOrReplaceWorker(root, bounds);
            // TODO: Re-enable subsumption when async bug is fixed
            // Optional<ConceptMap> subsumer = subsumptionTrackers.computeIfAbsent(
            //         root, r -> new SubsumptionTracker()).getSubsumer(bounds);
            // // If there is a finished subsumer, let the Worker know that it can go there for answers
            // Request request = subsumer
            //         .map(conceptMap -> Request.ToSubsumed.create(
            //                 driver(), worker, workersByRoot.get(root).get(conceptMap),
            //                 fromUpstream.partialAnswer()))
            //         .orElseGet(() -> Request.create(driver(), worker, fromUpstream.partialAnswer()));
            Request request = Request.create(driver(), worker, fromUpstream.partialAnswer());
            requestMapByRoot.computeIfAbsent(root, r -> new HashMap<>()).put(request, fromUpstream);
            requestFromDownstream(request, fromUpstream, iteration);
        }
    }

    private void prepareNextIteration(Driver<? extends Resolver<?>> root, int iteration) {
        iterationByRoot.put(root, iteration);
        cacheRegistersByRoot.remove(root);
        requestMapByRoot.remove(root);
        subsumptionTrackers.remove(root);
        workersByRoot.remove(root);
    }

    abstract Driver<WORKER> getOrReplaceWorker(Driver<? extends Resolver<?>> root, ConceptMap bounds);

    @Override
    protected void receiveAnswer(Answer answer, int iteration) {
        if (iteration < iterationByRoot.get(answer.answer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(answer.sourceRequest(), iteration);
        } else {
            answerToUpstream(answer.answer(),
                             requestMapByRoot.get(answer.answer().root()).get(answer.sourceRequest()),
                             iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fail, int iteration) {
        if (iteration < iterationByRoot.get(fail.sourceRequest().partialAnswer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fail.sourceRequest(), iteration);
        } else {
            Request request = fail.sourceRequest();
            subsumptionTrackers
                    .computeIfAbsent(request.partialAnswer().root(), r -> new SubsumptionTracker())
                    .addFinished(request.partialAnswer().conceptMap());
            failToUpstream(requestMapByRoot.get(fail.sourceRequest().partialAnswer().root()).get(request),
                           iteration);
        }
    }

}
