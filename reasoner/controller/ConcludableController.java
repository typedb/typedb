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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.BufferBroadcastReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveBase;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class ConcludableController extends Controller<ConceptMap, Map<Variable, Concept>, ConceptMap,
        ConcludableController.ConcludableProcessor, ConcludableController> {

    private final LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Variable.Retrievable> unboundVars;
    private final ControllerRegistry registry;
    private final Concludable concludable;

    public ConcludableController(Driver<ConcludableController> driver, Concludable concludable,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")", executorService, registry);
        this.registry = registry;
        this.concludable = concludable;
        this.unboundVars = unboundVars();
        this.upstreamConclusions = initialiseUpstreamConclusions(concludable);
    }

    private LinkedHashMap<Conclusion, Set<Unifier>> initialiseUpstreamConclusions(Concludable c) {
        LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions = new LinkedHashMap<>();
        c.getApplicableRules(registry.conceptManager(), registry.logicManager())
                .forEachRemaining(rule -> c.getUnifiers(rule).forEachRemaining(unifier -> {
                    // TODO: Do we need to terminate the actor here? We did in the previous model
                    try {
                        registry.registerConclusionController(rule.conclusion());
                        upstreamConclusions.computeIfAbsent(rule.conclusion(), r -> new HashSet<>()).add(unifier);
                    } catch (TypeDBException e) {
                        terminate(e);
                    }
                }));
        return upstreamConclusions;
    }

    private Set<Variable.Retrievable> unboundVars() {
        Set<Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(concludable.pattern().variables())
                .filter(var -> var.id().isRetrievable())
                .forEachRemaining(var -> {
                    if (var.isType() && !var.asType().label().isPresent()) {
                        missingBounds.add(var.asType().id().asRetrievable());
                    } else if (var.isThing() && !var.asThing().iid().isPresent()) {
                        missingBounds.add(var.asThing().id().asRetrievable());
                    }
                });
        return missingBounds;
    }

    @Override
    protected Function<Driver<ConcludableProcessor>, ConcludableProcessor> createProcessorFunc(ConceptMap bounds) {
        // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
        //  concludable. They should be filtered before being passed to the concludableProcessor's constructor
        return driver -> new ConcludableProcessor(
                driver, driver(), bounds, unboundVars, upstreamConclusions,
                () -> TraversalUtils.traversalIterator(registry, concludable.pattern(), bounds),
                ConcludableProcessor.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
        );
    }

    @Override
    protected <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, Map<Variable,
            Concept>, ConcludableProcessor, REQ>, PUB_C extends Controller<PUB_PROC_ID, ?, Map<Variable, Concept>, ?,
            PUB_C>> Connection.Builder<PUB_PROC_ID, Map<Variable, Concept>, ?, ?, ?> createBuilder(REQ req) {
        return null;
    }

    protected static class ConclusionRequest extends Processor.Request<Conclusion, ConceptMap, ConclusionController,
                    Map<Variable, Concept>, ConcludableProcessor, ConclusionRequest> {

        public ConclusionRequest(Driver<ConcludableProcessor> recProcessor, long recEndpointId,
                                 Conclusion provControllerId, ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, Map<Variable, Concept>, ConclusionRequest, ConcludableProcessor, ?> getBuilder(ControllerRegistry registry) {
            return createConnectionBuilder(registry.registerConclusionController(pubControllerId()));
        }
    }

    protected static class ConcludableProcessor extends Processor<Map<Variable, Concept>, ConceptMap, ConcludableProcessor> {

        private final ConceptMap bounds;
        private final Set<Variable.Retrievable> unboundVars;
        private final LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final Set<ConclusionRequest> requestedConnections;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver, Driver<ConcludableController> controller,
                                    ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                                    LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier, String name) {
            super(driver, controller, new BufferBroadcastReactive<>(new HashSet<>(), name), name);
            this.bounds = bounds;
            this.unboundVars = unboundVars;
            this.upstreamConclusions = upstreamConclusions;
            this.traversalSuppplier = traversalSuppplier;
            this.requestedConnections = new HashSet<>();
        }

        @Override
        public void setUp() {
            boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);
            ReactiveBase<ConceptMap, ConceptMap> op = noOp(name());
            if (singleAnswerRequired) op.findFirst().publishTo(outlet());
            else op.publishTo(outlet());

            Source.fromIteratorSupplier(traversalSuppplier, name()).publishTo(op);

            upstreamConclusions.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    InletEndpoint<Map<Variable, Concept>> endpoint = this.createReceivingEndpoint();
                    mayRequestConnection(new ConclusionRequest(driver(), endpoint.id(), conclusion, boundsAndRequirements.first()));
                    endpoint.flatMapOrRetry(conclusionAns -> unifier.unUnify(conclusionAns, boundsAndRequirements.second()))
                            .publishTo(op);
                }));
            });
        }

        private void mayRequestConnection(ConclusionRequest conclusionRequest) {
            if (!requestedConnections.contains(conclusionRequest)) {
                requestedConnections.add(conclusionRequest);
                requestConnection(conclusionRequest);
            }
        }
    }
}
