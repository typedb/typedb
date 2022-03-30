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

package com.vaticle.typedb.core.reasoner.computation.reactive.stream;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.AbstractPublisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public abstract class SingleReceiverStream<INPUT, OUTPUT> extends AbstractPublisher<OUTPUT> implements Reactive.Stream<INPUT, OUTPUT> {

    private final ReceiverRegistry.Single<Receiver.Subscriber<OUTPUT>> receiverRegistry;

    protected SingleReceiverStream(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.receiverRegistry = new ReceiverRegistry.Single<>();
    }

    protected abstract ProviderRegistry<Publisher<INPUT>> providerRegistry();

    @Override
    protected ReceiverRegistry.Single<Receiver.Subscriber<OUTPUT>> receiverRegistry() {
        return receiverRegistry;
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), identifier(), packet));
        providerRegistry().recordReceive(publisher);
    }

    @Override
    public void registerProvider(Publisher<INPUT> provider) {
        if (providerRegistry().add(provider)) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
        }
        if (receiverRegistry().isPulling()) provider.pull(this);
    }

    @Override
    public void registerReceiver(Receiver.Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.registerProvider(this);
    }

    public void sendTo(Receiver.Subscriber<OUTPUT> receiver) {
        // Allows sending of data without the downstream being able to pull from here
        receiverRegistry().addReceiver(receiver);
    }

}
