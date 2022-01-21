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

import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class BufferBroadcastReactive<PACKET> extends ReactiveImpl<PACKET, PACKET> {

    final Map<Receiver<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final Set<PACKET> bufferSet;
    final List<PACKET> bufferList;
    final Set<Receiver<PACKET>> pullers;
    protected final Set<Receiver<PACKET>> subscribers;
    private final MultiManager<PACKET> providerManager;

    public BufferBroadcastReactive(PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.pullers = new HashSet<>();
        this.subscribers = new HashSet<>();
        this.providerManager = new Provider.MultiManager<>(this, monitor());
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerManager.receivedFrom(provider);
        assert subscribers.size() > 0;
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            Set<Receiver<PACKET>> toSend = new HashSet<>(pullers);
            finishPulling();
            toSend.forEach(this::send);
        } else if (isPulling()) {
            providerManager.pull(provider);
            monitor().onPathTerminate();
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        bufferPositions.putIfAbsent(receiver, 0);
        subscribers.add(receiver);
        if (bufferList.size() == bufferPositions.get(receiver)) {
            // Finished the buffer
            setPulling(receiver);
            providerManager.pullAll();  // TODO: This pulls from all providers regardless of whether they're already pulling. This introduces double-pulls which we can't undo. Needs fixing.
        } else {
            send(receiver);
        }
    }

    // TODO: Should pulling methods be abstracted into a reactive interface? These calls are only used internally
    protected void finishPulling() {
        pullers.clear();
    }

    void setPulling(Receiver<PACKET> receiver) {
        pullers.add(receiver);
    }

    @Override
    protected boolean isPulling() {
        return pullers.size() > 0;
    }

    private void send(Receiver<PACKET> receiver) {
        Integer pos = bufferPositions.get(receiver);
        bufferPositions.put(receiver, pos + 1);
        receiver.receive(this, bufferList.get(pos));
    }

    @Override
    public void publishTo(Subscriber<PACKET> subscriber) {
        bufferPositions.putIfAbsent(subscriber, 0);
        subscribers.add(subscriber);
        subscriber.subscribeTo(this);
    }

    @Override
    public void subscribeTo(Provider<PACKET> provider) {
        providerManager.add(provider);
        if (isPulling()) providerManager.pull(provider);
    }

}
