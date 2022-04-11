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
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.BufferOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.FanOutOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.SubscriberRegistry;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class PoolingStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {  // TODO: We expect INPUT and OUTPUT to be the same for PoolingStreams

    private final Operator.Pool<INPUT, OUTPUT> pool;

    protected PoolingStream(Processor<?, ?, ?, ?> processor,
                            Operator.Pool<INPUT, OUTPUT> pool,
                            SubscriberRegistry<OUTPUT> subscriberRegistry,
                            PublisherRegistry<INPUT> publisherRegistry) {
        super(processor, subscriberRegistry, publisherRegistry);
        this.pool = pool;
    }

    public static <PACKET> PoolingStream<PACKET, PACKET> fanOut(
            Processor<?, ?, ?, ?> processor) {
        return new PoolingStream<>(processor, new FanOutOperator<>(), new SubscriberRegistry.Multi<>(),
                                   new PublisherRegistry.Single<>());
    }

    public static <INPUT, OUTPUT> PoolingStream<INPUT, OUTPUT> fanIn(
            Processor<?, ?, ?, ?> processor, Operator.Pool<INPUT, OUTPUT> pool) {
        return new PoolingStream<>(processor, pool, new SubscriberRegistry.Single<>(), new PublisherRegistry.Multi<>());
    }

    public static <PACKET> PoolingStream<PACKET, PACKET> fanInFanOut(Processor<?, ?, ?, ?> processor) {
        return new PoolingStream<>(processor, new FanOutOperator<>(), new SubscriberRegistry.Multi<>(),
                                   new PublisherRegistry.Multi<>());
    }

    public static <PACKET> PoolingStream<PACKET, PACKET> buffer(
            Processor<?, ?, ?, ?> processor) {
        // TODO: The operator is not bound to the nature of the registries by type. We could not correctly use a FanOut
        //  operator here even though the types allow it. In fact what really changes in tandem is the signature of the
        //  receive() and pull() methods, as when there are multiple upstreams/downstreams we need to know which the
        //  message is from/to, but  not so for single upstream/downstreams
        return new PoolingStream<>(processor, new BufferOperator<>(), new SubscriberRegistry.Single<>(),
                                   new PublisherRegistry.Single<>());
    }

    protected Operator.Pool<INPUT, OUTPUT> operator() {
        return pool;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        publisherActions.tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        // TODO: We don't care about the subscriber here
        if (operator().hasNext(subscriber)) {
            // TODO: Code duplicated in Source
            subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
            Operator.Supplied<OUTPUT> supplied = operator().next(subscriber);
            publisherActions.processEffects(supplied);
            publisherActions.subscriberReceive(subscriber, supplied.output());  // TODO: If the operator isn't tracking which subscribers have seen this packet then it needs to be sent to all subscribers. So far this is never the case.
        } else {
            publisherRegistry().nonPulling().forEach(this::propagatePull);
        }
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT packet) {
        subscriberActions.traceReceive(publisher, packet);
        publisherRegistry().recordReceive(publisher);

        publisherActions.processEffects(operator().accept(publisher, packet));
        AtomicBoolean retry = new AtomicBoolean();
        retry.set(false);
        iterate(subscriberRegistry().pulling()).forEachRemaining(subscriber -> {
            if (operator().hasNext(subscriber)) {
                Operator.Supplied<OUTPUT> supplied = operator().next(subscriber);
                publisherActions.processEffects(supplied);
                subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
                publisherActions.subscriberReceive(subscriber, supplied.output());
            } else {
                retry.set(true);
            }
        });
        if (retry.get()) subscriberActions.rePullPublisher(publisher);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return publisherActions.map(this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return publisherActions.flatMap(this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> distinct() {
        return publisherActions.distinct(this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return publisherActions.buffer(this);
    }

}
