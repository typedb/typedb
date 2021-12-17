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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.IdentityReactive.noOp;

public class RetrievableController extends Controller<Void, ConceptMap, RetrievableController.RetrievableProcessor, RetrievableController> {

    private final Retrievable retrievable;
    private final ControllerRegistry registry;

    public RetrievableController(Driver<RetrievableController> driver, String name, Retrievable retrievable,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService);
        this.retrievable = retrievable;
        this.registry = registry;
    }

    @Override
    protected Function<Driver<RetrievableProcessor>, RetrievableProcessor> createProcessorFunc(ConceptMap conceptMap) {
        return driver -> new RetrievableProcessor(driver, driver(), processorName(conceptMap), () -> traversalIterator(retrievable.pattern(), conceptMap));
    }

    private String processorName(ConceptMap conceptMap) {
        return RetrievableProcessor.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ", bounds: " + conceptMap.toString() + ")";
    }


    @Override
    protected Processor.ConnectionBuilder<Void, ConceptMap, ?, ?> getPublisherController(Processor.ConnectionRequest<Void, ConceptMap, ?> connectionRequest) {
        throw TypeDBException.of(ILLEGAL_STATE);  // TODO: Can we use typing to remove this?
    }

    protected FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, com.vaticle.typedb.core.concept.answer.ConceptMap bounds) {
        return compatibleBounds(conjunction, bounds).map(c -> {
            GraphTraversal.Thing traversal = boundTraversal(conjunction.traversal(), c);
            return registry.traversalEngine().iterator(traversal).map(v -> registry.conceptManager().conceptMap(v));
        }).orElse(Iterators.empty());
    }

    private Optional<ConceptMap> compatibleBounds(Conjunction conjunction, ConceptMap bounds) {
        Map<Identifier.Variable.Retrievable, Concept> newBounds = new HashMap<>();
        for (Map.Entry<Identifier.Variable.Retrievable, ? extends Concept> entry : bounds.concepts().entrySet()) {
            Identifier.Variable.Retrievable id = entry.getKey();
            Concept bound = entry.getValue();
            Variable conjVariable = conjunction.variable(id);
            assert conjVariable != null;
            if (conjVariable.isThing()) {
                if (!conjVariable.asThing().iid().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asThing().iid().get().iid().equals(bound.asThing().getIID())) {
                    return Optional.empty();
                }
            } else if (conjVariable.isType()) {
                if (!conjVariable.asType().label().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asType().label().get().properLabel().equals(bound.asType().getLabel())) {
                    return Optional.empty();
                }
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
        return Optional.of(new ConceptMap(newBounds));
    }

    protected static GraphTraversal.Thing boundTraversal(GraphTraversal.Thing traversal, ConceptMap bounds) {
        bounds.concepts().forEach((id, concept) -> {
            if (concept.isThing()) traversal.iid(id.asVariable(), concept.asThing().getIID());
            else {
                traversal.clearLabels(id.asVariable());
                traversal.labels(id.asVariable(), concept.asType().getLabel());
            }
        });
        return traversal;
    }

    public static class RetrievableProcessor extends Processor<ConceptMap, Void, RetrievableProcessor> {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableProcessor(Driver<RetrievableProcessor> driver,
                                       Driver<? extends Controller<Void, ConceptMap, RetrievableProcessor, ?>> controller,
                                       String name, Supplier<FunctionalIterator<ConceptMap>> traversalSupplier) {
            super(driver, controller, name, noOp());
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            new Source<>(traversalSupplier).publishTo(outlet());
        }
    }
}
