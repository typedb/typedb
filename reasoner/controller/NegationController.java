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
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.controller.NegationController.ReactiveBlock.Request;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.Input;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Subscriber.Finishable;
import com.vaticle.typedb.core.reasoner.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.reactive.common.SubscriberRegistry;

import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class NegationController extends AbstractController<
        ConceptMap,
        ConceptMap,
        ConceptMap,
        Request,
        NegationController.ReactiveBlock,
        NegationController
        > {

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
    protected ReactiveBlock createReactiveBlockFromDriver(Driver<ReactiveBlock> reactiveBlockDriver, ConceptMap bounds) {
        return new ReactiveBlock(
                reactiveBlockDriver, driver(), monitor, negated, bounds,
                () -> ReactiveBlock.class.getSimpleName() + "(pattern: " + negated + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public void resolveController(Request req) {
        if (isTerminated()) return;
        disjunctionContoller.execute(actor -> actor.resolveReactiveBlock(
                new AbstractReactiveBlock.Connector<>(req.inputId(), req.bounds())
        ));
    }

    protected static class ReactiveBlock
            extends AbstractReactiveBlock<ConceptMap, ConceptMap, Request, ReactiveBlock> {

        private final Negated negated;
        private final ConceptMap bounds;
        private NegationStream negation;

        protected ReactiveBlock(Driver<ReactiveBlock> driver, Driver<NegationController> controller,
                                Driver<Monitor> monitor, Negated negated, ConceptMap bounds,
                                Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.negated = negated;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanOut(this));
            Input<ConceptMap> input = createInput();
            requestConnection(new Request(input.identifier(), negated.pattern(), bounds));
            negation = new NegationStream(this, bounds);
            monitor().execute(actor -> actor.registerRoot(driver(), negation.identifier()));
            input.registerSubscriber(negation);
            negation.registerSubscriber(outputRouter());
        }

        @Override
        public void onFinished(Reactive.Identifier<?, ?> finishable) {
            assert !done;
//            done = true;
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
            // TODO: Negation should be modelled as both a source and a sink, called a Bridge for now. It is a Source
            //  for the graph downstream and a Sink for the graph upstream

            private final ConceptMap bounds;
            private boolean answerFound;

            protected NegationStream(AbstractReactiveBlock<?, ?, ?, ?> reactiveBlock, ConceptMap bounds) {
                super(reactiveBlock, new NegationOperator<>(), new SubscriberRegistry.Single<>(),
                      new PublisherRegistry.Single<>());
                this.bounds = bounds;
                this.answerFound = false;
            }

            @Override
            public void pull(Subscriber<ConceptMap> subscriber) {
                // TODO: This would create duplicate traces, but removing it is also a misnomer
                // Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
                if (!answerFound) super.pull(subscriber);
            }

            @Override
            public void receive(Publisher<ConceptMap> publisher, ConceptMap conceptMap) {
                subscriberActions.traceReceive(publisher, conceptMap);
                publisherRegistry().recordReceive(publisher);
                answerFound = true;
                reactiveBlock().monitor().execute(actor -> actor.rootFinalised(identifier()));
            }

            @Override
            public void finished() {
                assert !answerFound;
                reactiveBlock().monitor().execute(actor -> actor.createAnswer(identifier()));
                iterate(subscriberRegistry().subscribers()).forEachRemaining(r -> r.receive(this, bounds));
                reactiveBlock().monitor().execute(actor -> actor.rootFinalised(identifier()));
            }
        }

        protected static class Request extends Connector.AbstractRequest<Disjunction, ConceptMap, ConceptMap> {

            protected Request(Reactive.Identifier<ConceptMap, ?> inputId, Disjunction controllerId,
                              ConceptMap reactiveBlockId) {
                super(inputId, controllerId, reactiveBlockId);
            }

        }
    }

}
