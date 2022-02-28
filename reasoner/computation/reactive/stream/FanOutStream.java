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
    private final ProviderRegistry<PACKET> providerManager;
    private final ReceiverRegistry.MultiReceiverRegistry<PACKET> receiverRegistry;

    public FanOutStream(TerminationTracker monitor, String groupName) {
        super(monitor, groupName);
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.providerManager = new ProviderRegistry.SingleProviderRegistry<>(this);
        this.receiverRegistry = new ReceiverRegistry.MultiReceiverRegistry<>();
    }

    @Override
    protected ReceiverRegistry.MultiReceiverRegistry<PACKET> receiverRegistry() {
        return receiverRegistry;
    }

    protected ProviderRegistry<PACKET> providerRegistry() {
        return providerManager;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerRegistry().recordReceive(provider);
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            // assert receiverRegistry().monitors().size() > 0;  // TODO: Should we assert this?
            receiverRegistry().monitors().forEach(monitor -> {
                final int numCreated = receiverRegistry().size(monitor) - 1;
                if (numCreated > 0) tracker().reportAnswerCreate(numCreated, this, monitor);  // We need to account for sending an answer to all receivers (-1 for the one we received), either now or when they next pull.
            });
            Set<Receiver<PACKET>> toSend = receiverRegistry().pullingReceivers();
            receiverRegistry().recordReceive();
            toSend.forEach(this::send);
        } else {
            if (receiverRegistry().isPulling()) providerRegistry().pull(provider, receiverRegistry().monitors());
            tracker().onAnswerDestroy(this);  // When an answer is a duplicate then destroy it
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver, Set<Processor.Monitor.Reference> monitors) {
        bufferPositions.putIfAbsent(receiver, 0);
        Set<Processor.Monitor.Reference> newMonitors = receiverRegistry().recordPull(receiver, monitors);
        newMonitors.forEach(this::onNewReceiverOrMonitor);
        if (bufferList.size() == bufferPositions.get(receiver)) {
            // Finished the buffer
            providerRegistry().pullAll(receiverRegistry().monitors());
        } else {
            send(receiver);
        }
    }

    private void send(Receiver<PACKET> receiver) {  // TODO: Naming should show this updates the buffer position
        Integer pos = bufferPositions.get(receiver);
        bufferPositions.put(receiver, pos + 1);
        receiver.receive(this, bufferList.get(pos));
    }

    private void onNewReceiverOrMonitor(Processor.Monitor.Reference monitor) {
        if (receiverRegistry().size(monitor) > 1) {
            if (bufferSet.size() > 0) tracker().reportAnswerCreate(bufferSet.size(), this, monitor);  // New receiver, so any answer in the buffer will be dispatched there at some point
            tracker().reportPathJoin(this, monitor);
        }
        // We want to report joins to our tracker and parent monitors, but we should report a join to a monitor if we have more than one receiver that reports to that monitor
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
        if (receiverRegistry().isPulling()) providerRegistry().pull(provider, receiverRegistry().monitors());
    }

}
