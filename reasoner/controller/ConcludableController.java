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
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveBase;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class ConcludableController extends Controller<Conclusion, ConceptMap,
        ConcludableController.ConcludableProcessor, ConcludableController> {

    private final LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private final ControllerRegistry registry;
    private final Concludable concludable;

    public ConcludableController(Driver<ConcludableController> driver, Concludable concludable,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")", executorService);
        this.registry = registry;
        this.upstreamConclusions = initialiseUpstreamConclusions();
        this.unboundVars = unboundVars();
        this.concludable = concludable;
    }

    private LinkedHashMap<Conclusion, Set<Unifier>> initialiseUpstreamConclusions() {
        LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions = new LinkedHashMap<>();
        concludable.getApplicableRules(registry.conceptManager(), registry.logicManager())
                .forEachRemaining(rule -> concludable.getUnifiers(rule).forEachRemaining(unifier -> {
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

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
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
                driver, driver(),
                ConcludableProcessor.class.getSimpleName() + "(pattern: " + concludable + ", bounds: " + bounds + ")",
                bounds, unboundVars, upstreamConclusions,
                () -> TraversalUtils.traversalIterator(registry, concludable.pattern(), bounds)
        );
    }

    @Override
    protected ConnectionBuilder<Conclusion, ConceptMap, ?, ?> getProviderController(ConnectionRequest<Conclusion, ConceptMap, ?> connectionRequest) {
        return connectionRequest.createConnectionBuilder(registry.registerConclusionController(connectionRequest.pubControllerId()));
    }

    protected static class ConcludableProcessor extends Processor<ConceptMap, Conclusion, ConcludableProcessor> {

        private final ConceptMap bounds;
        private final Set<Identifier.Variable.Retrievable> unboundVars;
        private final LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver, Driver<ConcludableController> controller,
                                    String name, ConceptMap bounds, Set<Identifier.Variable.Retrievable> unboundVars,
                                    LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier) {
            super(driver, controller, name, noOp());
            this.bounds = bounds;
            this.unboundVars = unboundVars;
            this.upstreamConclusions = upstreamConclusions;
            this.traversalSuppplier = traversalSuppplier;
        }

        @Override
        public void setUp() {
            boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);
            ReactiveBase<ConceptMap, ConceptMap> op = noOp();
            if (singleAnswerRequired) op.findFirst().publishTo(outlet());
            else op.publishTo(outlet());

            Source.fromIteratorSupplier(traversalSuppplier).publishTo(op);

            upstreamConclusions.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    requestConnection(driver(), conclusion, boundsAndRequirements.first())
                            .flatMapOrRetry(conclusionAns -> unifier.unUnify(unpack(conclusionAns), boundsAndRequirements.second()))
                            .publishTo(op);
                }));
            });
        }

        private static Map<Identifier.Variable, Concept> unpack(ConceptMap conceptMap) {
            // return conceptMap.concepts();  // TODO: Doesn't work, could be a big issue
            // TODO: Presents the good question: should conclusions be identifiable by ConceptMap, or more precisely by Map<Variable, Concept>?
            return null;
        }
    }
}
