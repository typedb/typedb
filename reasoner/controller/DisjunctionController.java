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
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
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
        PROCESSOR extends DisjunctionController.DisjunctionProcessor<CONTROLLER, PROCESSOR>,
        CONTROLLER extends DisjunctionController<PROCESSOR, CONTROLLER>>
        extends Controller<ConceptMap, ConceptMap, ConceptMap, PROCESSOR, CONTROLLER> {

    private final List<Pair<Conjunction, Driver<NestedConjunctionController>>> conjunctionControllers;
    protected Disjunction disjunction;

    protected DisjunctionController(Driver<CONTROLLER> driver, Disjunction disjunction, ActorExecutorGroup executorService, Registry registry) {
        super(driver, executorService, registry,
              () -> DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction + ")");
        this.disjunction = disjunction;
        this.conjunctionControllers = new ArrayList<>();
    }

    @Override
    public void setUpUpstreamProviders() {
        disjunction.conjunctions().forEach(conjunction -> {
            Driver<NestedConjunctionController> controller = registry().registerNestedConjunctionController(conjunction);
            conjunctionControllers.add(new Pair<>(conjunction, controller));
        });
    }

    protected Driver<NestedConjunctionController> conjunctionProvider(Conjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class NestedConjunctionRequest<P extends DisjunctionProcessor<C, P>, C extends DisjunctionController<P, C>>
            extends ProviderRequest<Conjunction, ConceptMap, ConceptMap, P, C> {

        protected NestedConjunctionRequest(Reactive.Identifier.Input<ConceptMap> recEndpointId, Conjunction provControllerId,
                                           ConceptMap provProcessorId) {
            super(recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, ConceptMap> getConnectionBuilder(C controller) {
            return new Connection.Builder<>(controller.conjunctionProvider(providingControllerId()), this)
                    .withMap(c -> merge(c, providingProcessorId()));
        }
    }

    protected static abstract class DisjunctionProcessor<
            CONTROLLER extends DisjunctionController<PROCESSOR, CONTROLLER>,
            PROCESSOR extends DisjunctionProcessor<CONTROLLER, PROCESSOR>>
            extends Processor<ConceptMap, ConceptMap, CONTROLLER, PROCESSOR> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        protected DisjunctionProcessor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller,
                                       Driver<Monitor> monitor, Disjunction disjunction, ConceptMap bounds,
                                       Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            FanInStream<ConceptMap> fanIn = fanIn(this);
            setOutlet(getOutlet(fanIn));
            for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                InletEndpoint<ConceptMap> endpoint = createReceivingEndpoint();
                endpoint.publishTo(fanIn);
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestProvider(new DisjunctionController.NestedConjunctionRequest<>(endpoint.identifier(), conjunction, bounds.filter(retrievableConjunctionVars)));
            }
            fanIn.finaliseProviders();
        }

        protected Reactive.Stream<ConceptMap, ConceptMap> getOutlet(FanInStream<ConceptMap> fanIn) {
            // This method is only here to be overridden by root disjunction to avoid duplicating setUp
            return fanIn.buffer();
        }

    }

}
