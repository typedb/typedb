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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.TerminationTracker;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.AbstractPublisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.Set;

public abstract class SingleReceiverStream<INPUT, OUTPUT> extends AbstractPublisher<OUTPUT> implements Reactive.Stream<INPUT, OUTPUT> {

    private final ReceiverRegistry.SingleReceiverRegistry<OUTPUT> receiverRegistry;

    protected SingleReceiverStream(TerminationTracker monitor, String groupName) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        super(monitor, groupName);
        this.receiverRegistry = new ReceiverRegistry.SingleReceiverRegistry<>();
    }

    protected abstract ProviderRegistry<INPUT> providerRegistry();

    @Override
    protected ReceiverRegistry.SingleReceiverRegistry<OUTPUT> receiverRegistry() {
        return receiverRegistry;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerRegistry().recordReceive(provider);
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        assert receiver.equals(receiverRegistry().receiver());
        if (receiverRegistry().recordPull(receiver)) providerRegistry().pullAll();
    }

    @Override
    public void propagateMonitors(Receiver<OUTPUT> receiver, Set<Processor.Monitor.Reference> monitors) {
        providerRegistry().propagateMonitors(monitors);
    }

    @Override
    public void subscribeTo(Provider<INPUT> provider) {
        providerRegistry().add(provider);
        if (receiverRegistry().isPulling()) providerRegistry().pull(provider);
    }

    @Override
    public void publishTo(Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.subscribeTo(this);
    }

    public void sendTo(Receiver<OUTPUT> receiver) {
        // Allows sending of data without the downstream being able to pull from here
        receiverRegistry().addReceiver(receiver);
    }

}
