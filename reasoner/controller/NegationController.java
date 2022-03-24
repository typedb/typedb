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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Connector;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanOutStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.SingleReceiverSingleProviderStream;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.function.Supplier;

public class NegationController extends Controller<ConceptMap, ConceptMap, ConceptMap, NegationController.NegationProcessor, NegationController> {

    private final Negated negated;
    private final Driver<Monitor> monitor;
    private Driver<NestedDisjunctionController> disjunctionContoller;

    public NegationController(Driver<NegationController> driver, Negated negated, ActorExecutorGroup executorService,
                              Driver<Monitor> monitor, Registry registry) {
        super(driver, executorService, registry,
              () -> NegationController.class.getSimpleName() + "(pattern:" + negated + ")");
        this.negated = negated;
        this.monitor = monitor;
    }

    @Override
    public void setUpUpstreamControllers() {
        // TODO: If there is only one conjunction in the disjunction we could theoretically skip the disjunction, but
        //  this is architecturally difficult.
        disjunctionContoller = registry().registerNestedDisjunctionController(negated.pattern());
    }

    @Override
    protected NegationProcessor createProcessorFromDriver(Driver<NegationProcessor> processorDriver, ConceptMap bounds) {
        return new NegationProcessor(
                processorDriver, driver(), monitor, negated, bounds,
                () -> NegationProcessor.class.getSimpleName() + "(pattern: " + negated + ", bounds: " + bounds + ")"
        );
    }

    private Driver<NestedDisjunctionController> disjunctionContoller() {
        return disjunctionContoller;
    }

    @Override
    public NegationController getThis() {
        return this;
    }

    protected static class DisjunctionRequest extends ConnectionRequest<Disjunction, ConceptMap, ConceptMap, NegationController> {

        protected DisjunctionRequest(Reactive.Receiver.Input<ConceptMap> inputId, Disjunction controllerId,
                                     ConceptMap processorId) {
            super(inputId, controllerId, processorId);
        }

        @Override
        public Connector<ConceptMap, ConceptMap> getConnector(NegationController controller) {
            return new Connector<>(controller.disjunctionContoller(), this);
        }
    }

    protected static class NegationProcessor extends Processor<ConceptMap, ConceptMap, NegationController, NegationProcessor> {

        private final Negated negated;
        private final ConceptMap bounds;
        private NegationReactive negation;

        protected NegationProcessor(Driver<NegationProcessor> driver, Driver<NegationController> controller,
                                    Driver<Monitor> monitor, Negated negated, ConceptMap bounds,
                                    Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.negated = negated;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            setOutputRouter(new FanOutStream<>(this));
            Input<ConceptMap> input = createInput();
            requestConnection(new DisjunctionRequest(input.identifier(), negated.pattern(), bounds));
            negation = new NegationReactive(this, bounds);
            monitor().execute(actor -> actor.registerRoot(driver(), negation.identifier()));
            monitor().execute(actor -> actor.forkFrontier(1, negation.identifier()));
            input.registerSubscriber(negation);
            negation.registerSubscriber(outputRouter());
        }

        @Override
        protected void onFinished(Reactive.Identifier finishable) {
            assert !done;
//            done = true;
            assert finishable == negation.identifier();
            negation.finished();
        }

        private static class NegationReactive extends SingleReceiverSingleProviderStream<ConceptMap, ConceptMap> implements Reactive.Subscriber.Finishable<ConceptMap> {

            private final ConceptMap bounds;
            private boolean answerFound;

            protected NegationReactive(Processor<?, ?, ?, ?> processor, ConceptMap bounds) {
                super(processor);
                this.bounds = bounds;
                this.answerFound = false;
            }

            @Override
            public void pull(Subscriber<ConceptMap> subscriber) {
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
                if (!answerFound) super.pull(subscriber);
            }

            @Override
            public void receive(Publisher<ConceptMap> publisher, ConceptMap packet) {
                super.receive(publisher, packet);
                answerFound = true;
                processor().monitor().execute(actor -> actor.rootFinalised(identifier()));
            }

            @Override
            public void finished() {
                assert !answerFound;
                processor().monitor().execute(actor -> actor.createAnswer(identifier()));
                receiverRegistry().receiver().receive(this, bounds);
                processor().monitor().execute(actor -> actor.rootFinalised(identifier()));
            }
        }

    }

}
