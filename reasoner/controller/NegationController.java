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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.controller.NegationController.Processor.Request;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.Connector;
import com.vaticle.typedb.core.reasoner.processor.Input;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Subscriber.Finishable;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class NegationController extends AbstractController<
        ConceptMap,
        ConceptMap,
        ConceptMap,
        Request,
        NegationController.Processor,
        NegationController
        > {

    private final Negated negated;
    private Driver<NestedDisjunctionController> disjunctionContoller;

    public NegationController(Driver<NegationController> driver, Negated negated, Context context) {
        super(driver, context, () -> NegationController.class.getSimpleName() + "(pattern:" + negated + ")");
        this.negated = negated;
    }

    @Override
    public void setUpUpstreamControllers() {
        disjunctionContoller = registry().createNestedDisjunction(negated.pattern());
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver, ConceptMap bounds) {
        return new Processor(
                processorDriver, driver(), processorContext(), negated, bounds,
                () -> Processor.class.getSimpleName() + "(pattern: " + negated + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public void routeConnectionRequest(Request req) {
        if (isTerminated()) return;
        disjunctionContoller.execute(actor -> actor.establishProcessorConnection(
                new Connector<>(req.inputId(), req.bounds())
        ));
    }

    protected static class Processor
            extends AbstractProcessor<ConceptMap, ConceptMap, Request, Processor> {

        private final Negated negated;
        private final ConceptMap bounds;
        private NegationStream negation;

        protected Processor(Driver<Processor> driver, Driver<NegationController> controller, Context context,
                                Negated negated, ConceptMap bounds, Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.negated = negated;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            setInitialReactive(PoolingStream.fanOut(this));
            Input<ConceptMap> input = createInput();
            requestConnection(new Request(input.identifier(), negated.pattern(), bounds));
            negation = new NegationStream(this, bounds);
            monitor().execute(actor -> actor.registerRoot(driver(), negation.identifier()));
            input.registerSubscriber(negation);
            negation.registerSubscriber(outputRouter());
        }

        @Override
        public void onFinished(Reactive.Identifier<?, ?> finishable) {
            assert finishable == negation.identifier();
            negation.finished();
        }

        private static class NegationOperator<PACKET> implements Operator.Transformer<PACKET, PACKET> {

            @Override
            public Set<Publisher<PACKET>> initialNewPublishers() {
                return set();
            }

            @Override
            public Either<Publisher<PACKET>, Set<PACKET>> accept(Publisher<PACKET> publisher, PACKET packet) {
                return Either.second(set(packet));
            }
        }

        private static class NegationStream
                extends TransformationStream<ConceptMap, ConceptMap> implements Finishable<ConceptMap> {
            // TODO: Negation should be modelled as both a source and a sink, possible name: Bridge. It is a Source
            //  for the graph downstream and a Sink for the graph upstream

            private final ConceptMap bounds;
            private boolean answerFound;

            protected NegationStream(AbstractProcessor<?, ?, ?, ?> processor, ConceptMap bounds) {
                super(processor, new NegationOperator<>(), new SubscriberRegistry.Single<>(),
                      new PublisherRegistry.Single<>());
                this.bounds = bounds;
                this.answerFound = false;
            }

            @Override
            public void pull(Subscriber<ConceptMap> subscriber) {
                if (!answerFound) {
                    super.pull(subscriber);
                } else {
                    subscriberRegistry().recordPull(subscriber);
                    publisherActions.tracePull(subscriber);
                }
            }

            @Override
            public void receive(Publisher<ConceptMap> publisher, ConceptMap conceptMap) {
                subscriberActions.traceReceive(publisher, conceptMap);
                publisherRegistry().recordReceive(publisher);
                if (!answerFound) processor().monitor().execute(actor -> actor.rootFinished(identifier()));
                answerFound = true;
            }

            @Override
            public void finished() {
                assert !answerFound;
                processor().monitor().execute(actor -> actor.createAnswer(identifier()));
                iterate(subscriberRegistry().subscribers()).forEachRemaining(r -> r.receive(this, bounds));
                processor().monitor().execute(actor -> actor.sourceFinished(identifier()));
            }
        }

        protected static class Request extends Connector.AbstractRequest<Disjunction, ConceptMap, ConceptMap> {

            protected Request(Reactive.Identifier<ConceptMap, ?> inputId, Disjunction controllerId,
                              ConceptMap processorId) {
                super(inputId, controllerId, processorId);
            }

        }
    }

}
