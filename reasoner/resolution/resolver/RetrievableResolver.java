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
    private final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, Driver<BoundRetrievableResolver>>> boundRetrievablesByRoot; // TODO: We would like these not to be by root. They need to be, for now, for reiteration purposes.
    private final Map<Driver<? extends Resolver<?>>, Integer> iterationByRoot;
    private final Map<Driver<? extends Resolver<?>>, SubsumptionTracker> subsumptionTrackers;
    private final Map<Request, Request> requestMap;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.boundRetrievablesByRoot = new HashMap<>();
        this.iterationByRoot = new HashMap<>();
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
            Driver<BoundRetrievableResolver> boundRetrievable = getOrReplaceBoundRetrievable(root, bounds);
            Optional<ConceptMap> finishedSubsumingBounds = subsumptionTrackers.computeIfAbsent(
                    root, r -> new SubsumptionTracker()).getSubsumer(bounds);
            Request request;
            if (finishedSubsumingBounds.isPresent()) {
                // If there is a finished subsumer, let the BoundRetrievable know that it can go there for answers
                Driver<BoundRetrievableResolver> subsumer = boundRetrievablesByRoot.get(root).get(finishedSubsumingBounds.get());
                request = Request.ToSubsumed.create(driver(), boundRetrievable, subsumer, fromUpstream.partialAnswer());
            } else {
                request = Request.create(driver(), boundRetrievable, fromUpstream.partialAnswer());
            }
            requestMap.put(request, fromUpstream);
            requestFromDownstream(request, fromUpstream, iteration);
        }
    }

    private void prepareNextIteration(Driver<? extends Resolver<?>> root, int iteration) {
        iterationByRoot.put(root, iteration);
        boundRetrievablesByRoot.remove(root);
        subsumptionTrackers.remove(root);
        requestMap.clear();
    }

    private Driver<BoundRetrievableResolver> getOrReplaceBoundRetrievable(Driver<? extends Resolver<?>> root, ConceptMap bounds) {
        boundRetrievablesByRoot.computeIfAbsent(root, r -> new HashMap<>());
        return boundRetrievablesByRoot.get(root).computeIfAbsent(bounds, b -> {
            LOG.debug("{}: Creating a new BoundRetrievableResolver for bounds: {}", name(), bounds);
            return registry.registerBoundRetrievable(retrievable, bounds);
        });
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        if (iteration < iterationByRoot.get(fromDownstream.answer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromDownstream.sourceRequest(), iteration);
        } else {
            answerToUpstream(fromDownstream.answer(), requestMap.get(fromDownstream.sourceRequest()), iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        if (iteration < iterationByRoot.get(fromDownstream.sourceRequest().partialAnswer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromDownstream.sourceRequest(), iteration);
        } else {
            Request request = fromDownstream.sourceRequest();
            subsumptionTrackers
                    .computeIfAbsent(request.partialAnswer().root(), r -> new SubsumptionTracker())
                    .addFinished(request.partialAnswer().conceptMap());
            failToUpstream(requestMap.get(request), iteration); // TODO: Throws with nullpointer. Possible solution is scoping the requestMap by root so that not all are reset each iteration. However queries in the tests are serial so don't see how this can help
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

}
