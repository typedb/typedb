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

package com.vaticle.typedb.core.reasoner.controllers;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.reactive.ReactiveBase;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.reasoner.reactive.IdentityReactive.noOp;

public class ConcludableController extends Controller<Resolvable<?>, Conclusion, ConceptMap,
        ConcludableController.ConcludableProcessor, ConcludableController> {  // TODO: Note Resolvable not Concludable

    private final LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private final ControllerRegistry registry;

    protected ConcludableController(Driver<ConcludableController> driver, String name, Concludable id,
                                    ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, id, executorService);
        this.registry = registry;
        this.upstreamConclusions = initialiseUpstreamConclusions();
        this.unboundVars = unboundVars();
    }

    private LinkedHashMap<Conclusion, Set<Unifier>> initialiseUpstreamConclusions() {
        return null;  // TODO
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        return null;  // TODO
    }

    @Override
    protected Function<Driver<ConcludableProcessor>, ConcludableProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConcludableProcessor(driver, driver(), "", bounds, unboundVars, upstreamConclusions,
                                                  () -> traversalIterator(id().asConcludable().pattern(), bounds));  // TODO: WOuld be better not to have to do this cast
    }

    private FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return null;  // TODO
    }

    @Override
    protected ConnectionBuilder<Conclusion, ConceptMap, ?, ?> getPublisherController(ConnectionRequest<Conclusion, ConceptMap, ?> connectionRequest) {
        return connectionRequest.createConnectionBuilder(registry.registerConclusionController(connectionRequest.pubControllerId()));
    }

    @Override
    protected Driver<ConcludableProcessor> computeProcessorIfAbsent(ConnectionBuilder<?, ConceptMap, ?, ?> connectionBuilder) {
        // TODO: We can do subsumption here
        return processors.computeIfAbsent(connectionBuilder.publisherProcessorId(), this::buildProcessor);
    }

    public static class ConcludableProcessor extends Processor<ConceptMap, Conclusion, ConcludableProcessor> {

        public ConcludableProcessor(Driver<ConcludableProcessor> driver, Driver<ConcludableController> controller,
                                    String name, ConceptMap bounds, Set<Identifier.Variable.Retrievable> unboundVars,
                                    LinkedHashMap<Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier) {
            super(driver, controller, name, noOp());

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
            return null;
        }
    }
}
