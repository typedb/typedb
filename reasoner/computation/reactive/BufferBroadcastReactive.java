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

import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;

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
    protected final Set<Provider<PACKET>> publishers;

    public BufferBroadcastReactive(Set<Publisher<PACKET>> publishers, String groupName) {
        super(groupName);
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.pullers = new HashSet<>();
        this.subscribers = new HashSet<>();
        this.publishers = new HashSet<>();
        publishers.forEach(pub -> pub.publishTo(this));
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        assert subscribers.size() > 0;
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            Set<Receiver<PACKET>> toSend = new HashSet<>(pullers);
            finishPulling();
            toSend.forEach(this::send);
        } else if (isPulling()) {
            provider.pull(this);
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        bufferPositions.putIfAbsent(receiver, 0);
        subscribers.add(receiver);
        if (bufferList.size() == bufferPositions.get(receiver)) {
            // Finished the buffer
            setPulling(receiver);
            publishers.forEach(p -> p.pull(this));
        } else {
            send(receiver);
        }
    }

    @Override
    public void subscribeTo(Provider<PACKET> publisher) {
        publishers.add(publisher);
        if (isPulling()) publisher.pull(this);
    }

    // TODO: Should pulling methods be abstracted into a reactive interface? These calls are only used internally
    protected void finishPulling() {
        pullers.clear();
    }

    void setPulling(Receiver<PACKET> receiver) {
        pullers.add(receiver);
    }

    boolean isPulling() {
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

}
