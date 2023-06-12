/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.controller.DisjunctionController.Processor.Request;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.controller.ConjunctionController.merge;

public abstract class DisjunctionController<
        OUTPUT,
        PROCESSOR extends DisjunctionController.Processor<OUTPUT, PROCESSOR>,
        CONTROLLER extends DisjunctionController<OUTPUT, PROCESSOR, CONTROLLER>
        > extends AbstractController<ConceptMap, ConceptMap, OUTPUT, Request, PROCESSOR, CONTROLLER> {

    private final List<Pair<ResolvableConjunction, Driver<NestedConjunctionController>>> conjunctionControllers;
    private final Set<Retrievable> outputVariables;
    ResolvableDisjunction disjunction;

    DisjunctionController(Driver<CONTROLLER> driver, ResolvableDisjunction disjunction, Set<Identifier.Variable.Retrievable> outputVariables, Context context) {
        super(driver, context, () -> DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction.pattern() + ")");
        this.disjunction = disjunction;
        this.conjunctionControllers = new ArrayList<>();
        this.outputVariables = outputVariables;
    }

    @Override
    protected void setUpUpstreamControllers() {
        disjunction.conjunctions().forEach(conjunction -> {
            Driver<NestedConjunctionController> controller = registry().createNestedConjunction(conjunction, outputVariables);
            conjunctionControllers.add(new Pair<>(conjunction, controller));
        });
    }

    @Override
    public void routeConnectionRequest(Request req) {
        getConjunctionController(req.controllerId())
                .execute(actor -> actor.establishProcessorConnection(req.withMap(c -> merge(c, req.bounds()))));
    }

    private Driver<NestedConjunctionController> getConjunctionController(ResolvableConjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract static class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>>
            extends AbstractProcessor<ConceptMap, OUTPUT, Request, PROCESSOR> {

        private final ResolvableDisjunction disjunction;
        private final ConceptMap bounds;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends DisjunctionController<OUTPUT, PROCESSOR, ?>> controller,
                  Context context, ResolvableDisjunction disjunction, ConceptMap bounds,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            PoolingStream<ConceptMap> fanIn = new BufferStream<>(this);
            setHubReactive(getOrCreateHubReactive(fanIn));
            for (ResolvableConjunction conjunction : disjunction.conjunctions()) {
                InputPort<ConceptMap> input = createInputPort();
                input.registerSubscriber(fanIn);
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.pattern().variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestConnection(new Request(
                        input.identifier(), driver(), conjunction, bounds.filter(retrievableConjunctionVars)
                ));
            }
        }

        abstract Stream<OUTPUT,OUTPUT> getOrCreateHubReactive(Stream<ConceptMap, ConceptMap> fanIn);

        static class Request extends AbstractRequest<ResolvableConjunction, ConceptMap, ConceptMap> {

            Request(
                    Reactive.Identifier inputPortId, Driver<? extends Processor<?,?>> inputPortProcessor,
                    ResolvableConjunction controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

        }
    }

}
