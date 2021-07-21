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
 *
 */

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, Driver<BoundConcludableResolver>>> boundConcludableResolversByRoot; // TODO: We would like these not to be by root. They need to be, for now, for reiteration purposes.
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, AnswerCache<?, ConceptMap>>> cacheRegistersByRoot;
    protected final Map<Driver<? extends Resolver<?>>, Integer> iterationByRoot;
    private final Map<Driver<? extends Resolver<?>>, SubsumptionTracker> subsumptionTrackers;
    private final Map<Driver<? extends Resolver<?>>, Map<Request, Request>> requestMapByRoot;
    private boolean isInitialised;

    public ConcludableResolver(Driver<ConcludableResolver> driver, Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.isInitialised = false;
        this.boundConcludableResolversByRoot = new HashMap<>();
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
        if (iteration > iterationByRoot.get(root)) prepareNextIteration(root, iteration);
        else if (iteration < iterationByRoot.get(root)) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            ConceptMap bounds = fromUpstream.partialAnswer().conceptMap();
            Driver<BoundConcludableResolver> boundConcludable = getOrReplaceBoundConcludable(root, bounds);
            // TODO: Re-enable subsumption when async bug is fixed
            // Optional<ConceptMap> subsumer = subsumptionTrackers.computeIfAbsent(
            //         root, r -> new SubsumptionTracker()).getSubsumer(bounds);
            // // If there is a finished subsumer, let the BoundConcludable know that it can go there for answers
            // Request request = subsumer
            //         .map(conceptMap -> Request.ToSubsumed.create(
            //                 driver(), boundConcludable, boundConcludablesByRoot.get(root).get(conceptMap),
            //                 fromUpstream.partialAnswer()))
            //         .orElseGet(() -> Request.create(driver(), boundConcludable, fromUpstream.partialAnswer()));
            Request request = Request.create(driver(), boundConcludable, fromUpstream.partialAnswer());
            requestMapByRoot.computeIfAbsent(root, r -> new HashMap<>()).put(request, fromUpstream);
            requestFromDownstream(request, fromUpstream, iteration);
        }
    }

    private void prepareNextIteration(Driver<? extends Resolver<?>> root, int iteration) {
        iterationByRoot.put(root, iteration);
        cacheRegistersByRoot.remove(root);
        requestMapByRoot.remove(root);
    }

    private Driver<BoundConcludableResolver> getOrReplaceBoundConcludable(Driver<? extends Resolver<?>> root, ConceptMap bounds) {
        return boundConcludableResolversByRoot.computeIfAbsent(root, r -> new HashMap<>()).computeIfAbsent(bounds, b -> {
            LOG.debug("{}: Creating a new BoundConcludableResolver for bounds: {}", name(), bounds);
            // TODO: We could use the bounds to prune the applicable rules further
            return registry.registerBoundConcludable(concludable, bounds, resolverRules, applicableRules);
        });
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        if (iteration < iterationByRoot.get(fromDownstream.answer().root())) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromDownstream.sourceRequest(), iteration);
        } else {
            answerToUpstream(fromDownstream.answer(),
                             requestMapByRoot.get(fromDownstream.answer().root()).get(fromDownstream.sourceRequest()),
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

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        concludable.getApplicableRules(conceptMgr, logicMgr).forEachRemaining(rule -> concludable.getUnifiers(rule)
                .forEachRemaining(unifier -> {
                    if (isTerminated()) return;
                    try {
                        Driver<ConclusionResolver> conclusionResolver = registry.registerConclusion(rule.conclusion());
                        applicableRules.putIfAbsent(conclusionResolver, new HashSet<>());
                        applicableRules.get(conclusionResolver).add(unifier);
                        resolverRules.put(conclusionResolver, rule);
                    } catch (TypeDBException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

}
