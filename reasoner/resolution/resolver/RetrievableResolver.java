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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class RetrievableResolver extends Resolver<RetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;
    private final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, Driver<BoundRetrievableResolver>>> boundRetrievablesByRoot;
    private final Map<Driver<BoundRetrievableResolver>, Integer> boundRetrievableIterations;
    private final Map<Driver<? extends Resolver<?>>, Integer> iterationByRoot;
    private final Map<Request, Request> requestMap;
    private final Map<Driver<? extends Resolver<?>>, SubsumptionTracker> subsumptionTrackers;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.boundRetrievablesByRoot = new HashMap<>();
        this.iterationByRoot = new HashMap<>();
        this.boundRetrievableIterations = new HashMap<>();
        this.requestMap = new HashMap<>();  // TODO: We can do without this by specialising the message types
        this.subsumptionTrackers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;

        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        iterationByRoot.putIfAbsent(root, iteration);
        if (iteration > iterationByRoot.get(root)) {
            prepareNextIteration(root, iteration);
        }

        if (iteration < iterationByRoot.get(root)) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            ConceptMap bounds = fromUpstream.partialAnswer().conceptMap();
            Optional<ConceptMap> finishedSubsumingBounds = subsumptionTrackers.computeIfAbsent(
                    root, r -> new SubsumptionTracker()).getSubsumer(bounds);
            if (finishedSubsumingBounds.isPresent()) bounds = finishedSubsumingBounds.get();
            Driver<BoundRetrievableResolver> boundRetrievable = getOrReplaceBoundRetrievable(root, bounds, iteration);
            Request request = Request.create(driver(), boundRetrievable, fromUpstream.partialAnswer());
            requestMap.put(request, fromUpstream);
            requestFromDownstream(request, fromUpstream, iteration);
        }
    }

    private void prepareNextIteration(Driver<? extends Resolver<?>> root, int iteration) {
        iterationByRoot.put(root, iteration);
        boundRetrievablesByRoot.get(root).clear();
        // boundRetrievableIterations.clear(); // TODO: Clearing this causes a test failure
        subsumptionTrackers.remove(root);
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        answerToUpstream(fromDownstream.answer(), requestMap.get(fromDownstream.sourceRequest()), iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        subsumptionTrackers
                .computeIfAbsent(fromDownstream.sourceRequest().partialAnswer().root(), r -> new SubsumptionTracker())
                .addFinished(fromDownstream.sourceRequest().partialAnswer().conceptMap());
        failToUpstream(requestMap.get(fromDownstream.sourceRequest()), iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private Driver<BoundRetrievableResolver> getOrReplaceBoundRetrievable(Driver<? extends Resolver<?>> root,
                                                                          ConceptMap bounds, int iteration) {
        boundRetrievablesByRoot.computeIfAbsent(root, r -> new HashMap<>());
        Driver<BoundRetrievableResolver> boundRetrievable;
        if (!boundRetrievablesByRoot.get(root).containsKey(bounds)) {
            boundRetrievable = putBoundRetrievable(root, bounds, iteration);
        } else {
            boundRetrievable = boundRetrievablesByRoot.get(root).get(bounds);
            if (boundRetrievableIterations.get(boundRetrievable) < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                boundRetrievable = putBoundRetrievable(root, bounds, iteration);
            }
        }
        return boundRetrievable;
    }

    protected Driver<BoundRetrievableResolver> putBoundRetrievable(Driver<? extends Resolver<?>> root,
                                                                   ConceptMap bounds, int iteration) {
        LOG.debug("{}: Creating a new BoundRetrievableResolver for iteration:{}, bounds: {}", name(), iteration, bounds);
        Driver<BoundRetrievableResolver> boundRetrievable = registry.registerBoundRetrievable(retrievable, bounds);
        boundRetrievablesByRoot.get(root).put(bounds, boundRetrievable);
        boundRetrievableIterations.put(boundRetrievable, iteration);
        return boundRetrievable;
    }

}
