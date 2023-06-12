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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.controller.NegationController.Processor.Request;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Subscriber.Finishable;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanOut;

public class NegationController extends AbstractController<
        ConceptMap,
        ConceptMap,
        ConceptMap,
        Request,
        NegationController.Processor,
        NegationController
        > {

    private final Negated negated;
    private final Set<Identifier.Variable.Retrievable> outputVariables;
    private Driver<NestedDisjunctionController> disjunctionContoller;

    NegationController(Driver<NegationController> driver, Negated negated, Set<Identifier.Variable.Retrievable> outputVariables, Context context) {
        super(driver, context, () -> NegationController.class.getSimpleName() + "(pattern:" + negated + ")");
        this.negated = negated;
        this.outputVariables = outputVariables;
    }

    @Override
    public void setUpUpstreamControllers() {
        disjunctionContoller = registry().createNestedDisjunction(negated.disjunction(), outputVariables);
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
        disjunctionContoller.execute(actor -> actor.establishProcessorConnection(req));
    }

    protected static class Processor extends AbstractProcessor<ConceptMap, ConceptMap, Request, Processor> {

        private final Negated negated;
        private final ConceptMap bounds;
        private NegationStream negation;

        private Processor(Driver<Processor> driver, Driver<NegationController> controller, Context context,
                          Negated negated, ConceptMap bounds, Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.negated = negated;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            setHubReactive(fanOut(this));
            InputPort<ConceptMap> input = createInputPort();
            requestConnection(new Request(input.identifier(), driver(), negated.pattern(), bounds));
            negation = new NegationStream(this, bounds);
            monitor().execute(actor -> actor.registerRoot(driver(), negation.identifier()));
            input.registerSubscriber(negation);
            negation.registerSubscriber(hubReactive());
        }

        @Override
        public void onFinished(Reactive.Identifier finishable) {
            assert finishable == negation.identifier();
            negation.finished();
        }

        private static class NegationStream
                extends TransformationStream<ConceptMap, ConceptMap> implements Finishable<ConceptMap> {

            private final ConceptMap bounds;
            private boolean answerFound;

            private NegationStream(AbstractProcessor<?, ?, ?, ?> processor, ConceptMap bounds) {
                super(processor, new SubscriberRegistry.Single<>(),
                      new PublisherRegistry.Single<>());
                this.bounds = bounds;
                this.answerFound = false;
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(
                    Publisher<ConceptMap> publisher, ConceptMap packet
            ) {
                return Either.second(set(packet));
            }

            @Override
            public void pull(Subscriber<ConceptMap> subscriber) {
                if (!answerFound) {
                    super.pull(subscriber);
                } else {
                    subscriberRegistry().recordPull(subscriber);
                    publisherDelegate().tracePull(subscriber);
                }
            }

            @Override
            public void receive(Publisher<ConceptMap> publisher, ConceptMap conceptMap) {
                subscriberDelegate().traceReceive(publisher, conceptMap);
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

        static class Request extends AbstractRequest<
                Disjunction, ConceptMap, ConceptMap
                > {

            Request(
                    Reactive.Identifier inputPortId, Driver<Processor> inputPortProcessor,
                    Disjunction controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

        }
    }

}
