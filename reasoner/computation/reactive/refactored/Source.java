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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;

import java.util.function.Function;

public class Source<PACKET> extends ReactiveImpl implements Reactive.Publisher<PACKET> {

    private final Operator.Source<PACKET, Subscriber<PACKET>> supplierOperator;
    private final ReceiverRegistry.Single<Subscriber<PACKET>> receiverRegistry;
    private final ReactiveActions.PublisherActions<Subscriber<PACKET>, PACKET> providerActions;

    protected Source(Processor<?, ?, ?, ?> processor, Operator.Source<PACKET, Subscriber<PACKET>> supplierOperator,
                     ReceiverRegistry.Single<Subscriber<PACKET>> receiverRegistry,
                     ReactiveActions.PublisherActions<Subscriber<PACKET>, PACKET> providerActions) {
        super(processor);
        this.supplierOperator = supplierOperator;
        this.receiverRegistry = receiverRegistry;
        this.providerActions = providerActions;
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    public static <OUTPUT> Source<OUTPUT> create(
            Processor<?, ?, ?, ?> processor, Operator.Source<OUTPUT, Subscriber<OUTPUT>> operator) {
        return new Source<>(processor, operator, new ReceiverRegistry.Single<>(), new AbstractStream.PublisherActionsImpl<>(null));
    }

    private Operator.Source<PACKET, Subscriber<PACKET>> operator() {
        return supplierOperator;
    }

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        if (operator().hasNext(subscriber)) {
            // TODO: Code duplicated in PoolingStream
            providerActions.receiverRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
            Operator.Supplied<PACKET, Void> supplied = operator().next(subscriber);
            providerActions.processEffects(supplied);
            providerActions.outputToReceiver(subscriber, supplied.output());  // TODO: If the operator isn't tracking which receivers have seen this packet then it needs to be sent to all receivers. So far this is never the case.
        } else {
            processor().monitor().execute(actor -> actor.sourceFinished(identifier()));
        }
    }

    public ReceiverRegistry<Subscriber<PACKET>> receiverRegistry() {
        return receiverRegistry;
    }

    @Override
    public void registerReceiver(Subscriber<PACKET> subscriber) {
        receiverRegistry.addReceiver(subscriber);
        subscriber.registerProvider(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
        return providerActions.map(processor, this, function);
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
        return providerActions.flatMap(processor, this, function);
    }

    @Override
    public Stream<PACKET, PACKET> buffer() {
        return providerActions.buffer(processor, this);
    }

    @Override
    public Stream<PACKET, PACKET> deduplicate() {
        return providerActions.deduplicate(processor, this);
    }

}
