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

    final Map<Receiver.Subscriber<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final Set<PACKET> bufferSet;
    final List<PACKET> bufferList;
    private final ProviderRegistry.Single<Publisher<PACKET>> providerRegistry;
    private final ReceiverRegistry.Multi<Receiver.Subscriber<PACKET>> receiverRegistry;

    public FanOutStream(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.providerRegistry = new ProviderRegistry.Single<>();
        this.receiverRegistry = new ReceiverRegistry.Multi<>();
    }

    @Override
    protected ReceiverRegistry.Multi<Receiver.Subscriber<PACKET>> receiverRegistry() {
        return receiverRegistry;
    }

    protected ProviderRegistry.Single<Publisher<PACKET>> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), identifier(), packet));
        providerRegistry().recordReceive(publisher);
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            processor().monitor().execute(actor -> actor.createAnswer(identifier()));
            Set<Receiver.Subscriber<PACKET>> pullingReceivers = receiverRegistry().pulling();
            receiverRegistry().setNotPulling();
            pullingReceivers.forEach(this::sendFromBuffer);
        } else {
            if (receiverRegistry().anyPulling()) processor().schedulePullRetry(publisher, this);
        }
        processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
    }

    @Override
    public void pull(Receiver.Subscriber<PACKET> subscriber) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
        receiverRegistry().recordPull(subscriber);
        bufferPositions.putIfAbsent(subscriber, 0);
        if (bufferList.size() == bufferPositions.get(subscriber)) {
            // Finished the buffer
            if (receiverRegistry().anyPulling() && providerRegistry().setPulling()) providerRegistry().provider().pull(this);
        } else {
            sendFromBuffer(subscriber);
        }
    }

    private void sendFromBuffer(Receiver.Subscriber<PACKET> receiver) {
        Integer pos = bufferPositions.get(receiver);
        bufferPositions.put(receiver, pos + 1);
        receiver.receive(this, bufferList.get(pos));
    }

    @Override
    public void registerReceiver(Receiver.Subscriber<PACKET> subscriber) {
        bufferPositions.putIfAbsent(subscriber, 0);
        receiverRegistry().addReceiver(subscriber);
        subscriber.registerProvider(this);
    }

    @Override
    public void registerProvider(Publisher<PACKET> provider) {
        if (providerRegistry().add(provider)) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
        }
        if (receiverRegistry().anyPulling() && providerRegistry().setPulling()) provider.pull(this);
    }

}
