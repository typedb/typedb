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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.reasoner.reactive.common.ReactiveActions;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.function.Function;

public abstract class AbstractReactive implements Reactive {

    protected final ReactiveBlock<?, ?, ?, ?> reactiveBlock;
    protected final Reactive.Identifier<?, ?> identifier;

    protected AbstractReactive(ReactiveBlock<?, ?, ?, ?> reactiveBlock) {
        this.reactiveBlock = reactiveBlock;
        this.identifier = reactiveBlock().registerReactive(this);
    }

    @Override
    public Reactive.Identifier<?, ?> identifier() {
        return identifier;
    }

    @Override
    public ReactiveBlock<?, ?, ?, ?> reactiveBlock() {
        return reactiveBlock;
    }

    public static class SubscriberActionsImpl<INPUT> implements ReactiveActions.SubscriberActions<INPUT> {

        private final Subscriber<INPUT> subscriber;

        public SubscriberActionsImpl(Subscriber<INPUT> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void registerPath(Publisher<INPUT> publisher) {
            subscriber.reactiveBlock().monitor().execute(actor -> actor.registerPath(subscriber.identifier(), publisher.identifier()));
        }

        @Override
        public void traceReceive(Publisher<INPUT> publisher, INPUT packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), subscriber.identifier(), packet));
        }

        @Override
        public void rePullPublisher(Publisher<INPUT> publisher) {
            subscriber.reactiveBlock().schedulePullRetry(publisher, subscriber);
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
                publisher.reactiveBlock().monitor().execute(actor -> actor.createAnswer(publisher.identifier()));
            }
        }

        @Override
        public void monitorConsumeAnswers(int answersConsumed) {
            for (int i = 0; i < answersConsumed; i++) {
                publisher.reactiveBlock().monitor().execute(actor -> actor.consumeAnswer(publisher.identifier()));
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
            Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.reactiveBlock(), new Operator.Map<>(function));
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Publisher<OUTPUT> publisher,
                                                       Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.reactiveBlock(), new Operator.FlatMap<>(function));
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> distinct(Publisher<OUTPUT> publisher) {
            Stream<OUTPUT, OUTPUT> newOp = TransformationStream.single(publisher.reactiveBlock(), new Operator.Distinct<>());
            publisher.registerSubscriber(newOp);
            return newOp;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> buffer(Publisher<OUTPUT> publisher) {
            Stream<OUTPUT, OUTPUT> newOp = PoolingStream.buffer(publisher.reactiveBlock());
            publisher.registerSubscriber(newOp);
            return newOp;
        }
    }
}
