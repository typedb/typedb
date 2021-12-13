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

package com.vaticle.typedb.core.reasoner.reactive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class BufferBroadcastReactive<PACKET> extends ReactiveImpl<PACKET, PACKET> {

    final Map<Receiver<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final List<PACKET> buffer;
    final Set<Receiver<PACKET>> pullers;
    protected final Set<Receiver<PACKET>> subscribers;
    protected final Set<Provider<PACKET>> publishers;

    public BufferBroadcastReactive(Set<Provider<PACKET>> publishers) {
        this.buffer = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.pullers = new HashSet<>();
        this.subscribers = new HashSet<>();
        this.publishers = publishers;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        assert subscribers.size() > 0;
        buffer.add(packet);
        pullers.forEach(this::send);
        finishPulling();
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        createBufferPosition(receiver);
        subscribers.add(receiver);
        if (buffer.size() == bufferPositions.get(receiver)) {
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
        receiver.receive(this, buffer.get(pos));
        bufferPositions.put(receiver, pos + 1);
    }

    @Override
    public void publishTo(Subscriber<PACKET> subscriber) {
        createBufferPosition(subscriber);
        subscribers.add(subscriber);
        subscriber.subscribeTo(this);
    }

    private void createBufferPosition(Receiver<PACKET> subscriber) {
        if (!subscribers.contains(subscriber)) {
            bufferPositions.put(subscriber, 0);
        }
    }
}
