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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.BufferBroadcastReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveBase;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.utils.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.FanInReactive.fanIn;

public class ConcludableController extends Controller<ConceptMap, Map<Variable, Concept>, ConceptMap,
        ConcludableController.ConcludableProcessor, ConcludableController> {

    private final Map<Conclusion, Driver<ConclusionController>> conclusionControllers;
    private final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
    private final Set<Variable.Retrievable> unboundVars;
    private final Registry registry;
    private final Concludable concludable;

    public ConcludableController(Driver<ConcludableController> driver, Concludable concludable,
                                 ActorExecutorGroup executorService, Registry registry) {
        super(driver, executorService, registry,
              ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")");
        this.registry = registry;
        this.concludable = concludable;
        this.unboundVars = unboundVars();
        this.conclusionControllers = new HashMap<>();  // TODO: Any reason to use LinkedHashMap over HashMap?
        this.conclusionUnifiers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamProviders() {
        concludable.getApplicableRules(registry.conceptManager(), registry.logicManager())
                .forEachRemaining(rule -> {
                    Driver<ConclusionController> controller = registry.registerConclusionController(rule.conclusion());
                    conclusionControllers.put(rule.conclusion(), controller);
                    conclusionUnifiers.put(rule.conclusion(), concludable.getUnifiers(rule).toSet());
                });
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
                driver, driver(), bounds, unboundVars, conclusionUnifiers,
                () -> Traversal.traversalIterator(registry, concludable.pattern(), bounds),
                ConcludableProcessor.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
        );
    }

    private Driver<ConclusionController> conclusionProvider(Conclusion conclusion) {
        return conclusionControllers.get(conclusion);
    }

    @Override
    public ConcludableController asController() {
        return this;
    }

    protected static class ConcludableProcessor extends Processor<Map<Variable, Concept>, ConceptMap, ConcludableController, ConcludableProcessor> {

        private final ConceptMap bounds;
        private final Set<Variable.Retrievable> unboundVars;
        private final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final Set<ConclusionRequest> requestedConnections;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver, Driver<ConcludableController> controller,
                                    ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier, String name) {
            super(driver, controller, name);
            this.bounds = bounds;
            this.unboundVars = unboundVars;
            this.conclusionUnifiers = conclusionUnifiers;
            this.traversalSuppplier = traversalSuppplier;
            this.requestedConnections = new HashSet<>();
        }

        @Override
        public void setUp() {
            setOutlet(new BufferBroadcastReactive<>(this, name()));
            boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);
            Reactive<ConceptMap, ConceptMap> op = fanIn(this, name());
            if (singleAnswerRequired) op.buffer().findFirst().publishTo(outlet());
            else op.buffer().publishTo(outlet());

            Source.fromIteratorSupplier(traversalSuppplier, this, name()).publishTo(op);

            conclusionUnifiers.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    InletEndpoint<Map<Variable, Concept>> endpoint = this.createReceivingEndpoint();
                    mayRequestConnection(new ConclusionRequest(driver(), endpoint.id(), conclusion, boundsAndRequirements.first()));
                    endpoint.flatMapOrRetry(conclusionAns -> unifier.unUnify(conclusionAns, boundsAndRequirements.second()))
                            .buffer() // TODO: Included to combat flatMap overproducing.
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

        protected static class ConclusionRequest extends Request<Conclusion, ConceptMap, ConclusionController,
                        Map<Variable, Concept>, ConcludableProcessor, ConcludableController, ConclusionRequest> {

            public ConclusionRequest(Driver<ConcludableProcessor> recProcessor, long recEndpointId,
                                     Conclusion provControllerId, ConceptMap provProcessorId) {
                super(recProcessor, recEndpointId, provControllerId, provProcessorId);
            }

            @Override
            public Builder<ConceptMap, Map<Variable, Concept>, ConclusionRequest, ConcludableProcessor, ?> getBuilder(ConcludableController controller) {
                return new Builder<>(controller.conclusionProvider(providingControllerId()), this);
            }
        }
    }
}
