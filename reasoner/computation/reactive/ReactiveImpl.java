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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.DistinctOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.FlatMapOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.MapOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.common.ReactiveActions;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.function.Function;

public abstract class ReactiveImpl implements Reactive {

    protected final Processor<?, ?, ?, ?> processor;
    protected final Reactive.Identifier<?, ?> identifier;

    protected ReactiveImpl(Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
    }

    @Override
    public Reactive.Identifier<?, ?> identifier() {
        return identifier;
    }

    @Override
    public Processor<?, ?, ?, ?> processor() {
        return processor;
    }

    public static class SubscriberActionsImpl<INPUT> implements ReactiveActions.SubscriberActions<INPUT> {

        private final Subscriber<INPUT> subscriber;

        public SubscriberActionsImpl(Subscriber<INPUT> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void registerPath(Publisher<INPUT> publisher) {
            subscriber.processor().monitor().execute(actor -> actor.registerPath(subscriber.identifier(), publisher.identifier()));
        }

        @Override
        public void traceReceive(Publisher<INPUT> publisher, INPUT packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), subscriber.identifier(), packet));
        }

        @Override
        public void rePullPublisher(Publisher<INPUT> publisher) {
            subscriber.processor().schedulePullRetry(publisher, subscriber);
        }
    }

    public static class PublisherActionsImpl<OUTPUT> implements ReactiveActions.PublisherActions<OUTPUT> {

        private final Publisher<OUTPUT> publisher;

        public PublisherActionsImpl(Publisher<OUTPUT> publisher) {
            this.publisher = publisher;
        }

        @Override
        public void monitorCreateAnswers(int answersCreated) {
            for (int i = 0; i < answersCreated; i++) {
                publisher.processor().monitor().execute(actor -> actor.createAnswer(publisher.identifier()));
            }
        }

        @Override
        public void monitorConsumeAnswers(int answersConsumed) {
            for (int i = 0; i < answersConsumed; i++) {
                publisher.processor().monitor().execute(actor -> actor.consumeAnswer(publisher.identifier()));
            }
        }

        @Override
        public void subscriberReceive(Subscriber<OUTPUT> subscriber, OUTPUT packet) {
            subscriber.receive(publisher, packet);
        }

        @Override
        public void tracePull(Subscriber<OUTPUT> subscriber) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(subscriber.identifier(), publisher.identifier()));
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> map(Publisher<OUTPUT> publisher, Function<OUTPUT, MAPPED> function) {
            Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.processor(), new MapOperator<>(function));
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Publisher<OUTPUT> publisher,
                                                       Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.processor(), new FlatMapOperator<>(function));
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> distinct(Publisher<OUTPUT> publisher) {
            Stream<OUTPUT, OUTPUT> newOp = TransformationStream.single(publisher.processor(), new DistinctOperator<>());
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> buffer(Publisher<OUTPUT> publisher) {
            Stream<OUTPUT, OUTPUT> newOp = PoolingStream.buffer(publisher.processor());
            publisher.registerSubscriber(newOp);
            return newOp;
        }
    }
}
