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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FanOutStream<PACKET> extends AbstractPublisher<PACKET> implements Reactive.Stream<PACKET, PACKET> {

    final Map<Receiver<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final Set<PACKET> bufferSet;
    final List<PACKET> bufferList;
    private final ProviderRegistry<PACKET> providerRegistry;
    private final ReceiverRegistry.MultiReceiverRegistry<PACKET> receiverRegistry;

    public FanOutStream(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this, processor);
        this.receiverRegistry = new ReceiverRegistry.MultiReceiverRegistry<>(this);
    }

    @Override
    protected ReceiverRegistry.MultiReceiverRegistry<PACKET> receiverRegistry() {
        return receiverRegistry;
    }

    protected ProviderRegistry<PACKET> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerRegistry().recordReceive(provider);
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            processor().monitor().execute(actor -> actor.createAnswer(identifier()));
            Set<Receiver<PACKET>> toSend = receiverRegistry().pullingReceivers();
            receiverRegistry().setNotPulling();
            toSend.forEach(this::send);
        } else {
            if (receiverRegistry().isPulling()) providerRegistry().retry(provider);
        }
        processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        receiverRegistry().recordPull(receiver);
        bufferPositions.putIfAbsent(receiver, 0);
        if (bufferList.size() == bufferPositions.get(receiver)) {
            // Finished the buffer
            assert receiverRegistry().isPulling();
            providerRegistry().pullAll();
        } else {
            send(receiver);
        }
    }

    private void send(Receiver<PACKET> receiver) {  // TODO: Naming should show this updates the buffer position
        Integer pos = bufferPositions.get(receiver);
        bufferPositions.put(receiver, pos + 1);
        receiver.receive(this, bufferList.get(pos));
    }

    @Override
    public void publishTo(Subscriber<PACKET> subscriber) {
        bufferPositions.putIfAbsent(subscriber, 0);
        receiverRegistry().addReceiver(subscriber);
        subscriber.subscribeTo(this);
    }

    @Override
    public void subscribeTo(Provider<PACKET> provider) {
        providerRegistry().add(provider);
        if (receiverRegistry().isPulling()) providerRegistry().pull(provider);
    }

}
