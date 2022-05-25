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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.controller.DisjunctionController.Processor.Request;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
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
        PROCESSOR extends DisjunctionController.Processor<PROCESSOR>,
        CONTROLLER extends DisjunctionController<PROCESSOR, CONTROLLER>
        > extends AbstractController<ConceptMap, ConceptMap, ConceptMap, Request, PROCESSOR, CONTROLLER> {

    private final List<Pair<Conjunction, Driver<NestedConjunctionController>>> conjunctionControllers;
    Disjunction disjunction;

    DisjunctionController(Driver<CONTROLLER> driver, Disjunction disjunction, Context context) {
        super(driver, context, () -> DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction + ")");
        this.disjunction = disjunction;
        this.conjunctionControllers = new ArrayList<>();
    }

    @Override
    protected void setUpUpstreamControllers() {
        disjunction.conjunctions().forEach(conjunction -> {
            Driver<NestedConjunctionController> controller = registry().createNestedConjunction(conjunction);
            conjunctionControllers.add(new Pair<>(conjunction, controller));
        });
    }

    @Override
    public void routeConnectionRequest(Request req) {
        if (isTerminated()) return;
        getConjunctionController(req.controllerId())
                .execute(actor -> actor.establishProcessorConnection(req.withMap(c -> merge(c, req.bounds()))));
    }

    private Driver<NestedConjunctionController> getConjunctionController(Conjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract static class Processor<PROCESSOR extends Processor<PROCESSOR>>
            extends AbstractProcessor<ConceptMap, ConceptMap, Request, PROCESSOR> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends DisjunctionController<PROCESSOR, ?>> controller,
                  Context context, Disjunction disjunction, ConceptMap bounds,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            PoolingStream<ConceptMap> fanIn = PoolingStream.fanIn(this, new Operator.Buffer<>());
            setHubReactive(getOutputRouter(fanIn));
            for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                InputPort<ConceptMap> input = createInputPort();
                input.registerSubscriber(fanIn);
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestConnection(new Request(
                        input.identifier(), driver(), conjunction, bounds.filter(retrievableConjunctionVars)
                ));
            }
        }

        Reactive.Stream<ConceptMap, ConceptMap> getOutputRouter(Reactive.Stream<ConceptMap, ConceptMap> fanIn) {
            // This method is only here to be overridden by root disjunction to avoid duplicating setUp
            return fanIn;
        }

        static class Request extends AbstractRequest<Conjunction, ConceptMap, ConceptMap, NestedConjunctionController> {

            Request(
                    Reactive.Identifier inputPortId, Driver<? extends Processor<?>> inputPortProcessor,
                    Conjunction controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

        }
    }

}
