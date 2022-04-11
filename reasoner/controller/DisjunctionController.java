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
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.ReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Input;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
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
        REACTIVE_BLOCK extends DisjunctionController.DisjunctionReactiveBlock<REACTIVE_BLOCK>,
        CONTROLLER extends DisjunctionController<REACTIVE_BLOCK, CONTROLLER>>
        extends Controller<ConceptMap, ConceptMap, ConceptMap, DisjunctionController.DisjunctionReactiveBlock.NestedConjunctionRequest, REACTIVE_BLOCK, CONTROLLER> {

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
    public void resolveController(DisjunctionReactiveBlock.NestedConjunctionRequest req) {
        if (isTerminated()) return;
        getConjunctionController(req.controllerId())
                .execute(actor -> actor.resolveReactiveBlock(new ReactiveBlock.Connector<>(req.inputId(), req.bounds())
                                                                 .withMap(c -> merge(c, req.bounds()))));
    }

    protected Driver<NestedConjunctionController> getConjunctionController(Conjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected static abstract class DisjunctionReactiveBlock<REACTIVE_BLOCK extends DisjunctionReactiveBlock<REACTIVE_BLOCK>>
            extends ReactiveBlock<ConceptMap, ConceptMap, DisjunctionReactiveBlock.NestedConjunctionRequest, REACTIVE_BLOCK> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        protected DisjunctionReactiveBlock(Driver<REACTIVE_BLOCK> driver,
                                           Driver<? extends DisjunctionController<REACTIVE_BLOCK, ?>> controller,
                                           Driver<Monitor> monitor, Disjunction disjunction, ConceptMap bounds,
                                           Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            PoolingStream<ConceptMap, ConceptMap> fanIn = PoolingStream.fanIn(this, new Operator.Buffer<>());
            setOutputRouter(getOutputRouter(fanIn));
            for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                Input<ConceptMap> input = createInput();
                input.registerSubscriber(fanIn);
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestConnection(new NestedConjunctionRequest(
                        input.identifier(), conjunction, bounds.filter(retrievableConjunctionVars)));
            }
        }

        protected Reactive.Stream<ConceptMap, ConceptMap> getOutputRouter(Reactive.Stream<ConceptMap, ConceptMap> fanIn) {
            // This method is only here to be overridden by root disjunction to avoid duplicating setUp
            return fanIn;
        }

        protected static class NestedConjunctionRequest extends Connector.Request<Conjunction, ConceptMap, ConceptMap> {

            protected NestedConjunctionRequest(Reactive.Identifier<ConceptMap, ?> inputId, Conjunction controllerId,
                                               ConceptMap reactiveBlockId) {
                super(inputId, controllerId, reactiveBlockId);
            }

        }
    }

}
