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
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.computation.actor.Connector;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanInStream;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanInStream.fanIn;
import static com.vaticle.typedb.core.reasoner.controller.ConjunctionController.merge;

public abstract class DisjunctionController<
        PROCESSOR extends DisjunctionController.DisjunctionProcessor<PROCESSOR>,
        CONTROLLER extends DisjunctionController<PROCESSOR, CONTROLLER>>
        extends Controller<ConceptMap, ConceptMap, ConceptMap, DisjunctionController.NestedConjunctionRequest, PROCESSOR, CONTROLLER> {

    private final List<Pair<Conjunction, Driver<NestedConjunctionController>>> conjunctionControllers;
    protected Disjunction disjunction;

    protected DisjunctionController(Driver<CONTROLLER> driver, Disjunction disjunction,
                                    ActorExecutorGroup executorService, Registry registry) {
        super(driver, executorService, registry,
              () -> DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction + ")");
        this.disjunction = disjunction;
        this.conjunctionControllers = new ArrayList<>();
    }

    @Override
    public void setUpUpstreamControllers() {
        disjunction.conjunctions().forEach(conjunction -> {
            Driver<NestedConjunctionController> controller = registry().registerNestedConjunctionController(conjunction);
            conjunctionControllers.add(new Pair<>(conjunction, controller));
        });
    }

    @Override
    protected void resolveController(NestedConjunctionRequest req) {
        if (isTerminated()) return;
        getConjunctionController(req.controllerId())
                .execute(actor -> actor.resolveProcessor(new Connector<>(req.inputId(), req.bounds())
                                                                 .withMap(c -> merge(c, req.bounds()))));
    }

    protected Driver<NestedConjunctionController> getConjunctionController(Conjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected static class NestedConjunctionRequest extends Connector.ConnectionRequest<Conjunction, ConceptMap, ConceptMap> {

        protected NestedConjunctionRequest(Reactive.Identifier<ConceptMap, ?> inputId, Conjunction controllerId,
                                           ConceptMap processorId) {
            super(inputId, controllerId, processorId);
        }

    }

    protected static abstract class DisjunctionProcessor<PROCESSOR extends DisjunctionProcessor<PROCESSOR>>
            extends Processor<ConceptMap, ConceptMap, NestedConjunctionRequest, PROCESSOR> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        protected DisjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends DisjunctionController<PROCESSOR, ?>> controller,
                                       Driver<Monitor> monitor, Disjunction disjunction, ConceptMap bounds,
                                       Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            FanInStream<ConceptMap> fanIn = fanIn(this);
            setOutputRouter(getOutputRouter(fanIn));
            for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                Input<ConceptMap> input = createInput();
                input.registerSubscriber(fanIn);
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestConnection(new DisjunctionController.NestedConjunctionRequest(
                        input.identifier(), conjunction, bounds.filter(retrievableConjunctionVars)));
            }
            fanIn.finaliseProviders();
        }

        protected Reactive.Stream<ConceptMap, ConceptMap> getOutputRouter(FanInStream<ConceptMap> fanIn) {
            // This method is only here to be overridden by root disjunction to avoid duplicating setUp
            return fanIn.buffer();
        }

    }

}
