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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class PoolingStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

    private final Operator.Pool<INPUT, OUTPUT> pool;

    protected PoolingStream(Processor<?, ?, ?, ?> processor,
                            Operator.Pool<INPUT, OUTPUT> pool,
                            ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                            ProviderRegistry<Publisher<INPUT>> providerRegistry) {
        super(processor, receiverRegistry, providerRegistry);
        this.pool = pool;
    }

    public static <INPUT, OUTPUT> PoolingStream<INPUT, OUTPUT> fanOut(
            Processor<?, ?, ?, ?> processor, Operator.Pool<INPUT, OUTPUT> pool) {
        return new PoolingStream<>(processor, pool, new ReceiverRegistry.Multi<>(), new ProviderRegistry.Single<>());
    }

    public static <INPUT, OUTPUT> PoolingStream<INPUT, OUTPUT> buffer(
            Processor<?, ?, ?, ?> processor, Operator.Pool<INPUT, OUTPUT> pool) {
        // TODO: It's possible to choose the wrong pool operator here since the operator is not bound to the nature of the registries by type.
        return new PoolingStream<>(processor, pool, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
    }

    protected Operator.Pool<INPUT, OUTPUT> operator() {
        return pool;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        // TODO: We don't care about the subscriber here
        if (operator().hasNext(subscriber)) {
            // TODO: Code duplicated in Source
            receiverRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
            Operator.Supplied<OUTPUT> supplied = operator().next(subscriber);
            providerActions.processEffects(supplied);
            providerActions.subscriberReceive(subscriber, supplied.output());  // TODO: If the operator isn't tracking which receivers have seen this packet then it needs to be sent to all receivers. So far this is never the case.
        } else {
            // TODO: for POOLING but not for SOURCE
            providerRegistry().nonPulling().forEach(this::propagatePull);
        }
    }

    @Override
    public void receive(Publisher<INPUT> provider, INPUT packet) {
        receiverActions.traceReceive(provider, packet);
        providerRegistry().recordReceive(provider);

        providerActions.processEffects(operator().accept(provider, packet));
        AtomicBoolean retry = new AtomicBoolean();
        retry.set(false);
        iterate(receiverRegistry().pulling()).forEachRemaining(receiver -> {
            if (operator().hasNext(receiver)) {
                Operator.Supplied<OUTPUT> supplied = operator().next(receiver);
                providerActions.processEffects(supplied);
                receiverRegistry().setNotPulling(receiver);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
                providerActions.subscriberReceive(receiver, supplied.output());
            } else {
                retry.set(true);
            }
        });
        if (retry.get()) receiverActions.rePullProvider(provider);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return providerActions.map(this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return providerActions.flatMap(this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> distinct() {
        return providerActions.distinct(this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return providerActions.buffer(this);
    }

}
