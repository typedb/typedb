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


// TODO: Should split reactives into broadcasting ones (only this one) and ones which can have a single subscriber
// TODO: Single subscriber: can be dynamically set, but only set once. Calling receive before the subscriber is set should throw an error
public class BufferBroadcastReactive<PACKET> extends Reactive<PACKET, PACKET> {

    final Map<Receiver<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final List<PACKET> buffer;
    final Set<Receiver<PACKET>> pullers;

    protected BufferBroadcastReactive(Set<Provider<PACKET>> publishers) {
        super(publishers);
        this.buffer = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
        this.pullers = new HashSet<>();
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        buffer.add(packet);
        pullers.forEach(this::send);
        finishPulling();
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        createBufferPosition(receiver);
        subscribers.add(receiver);
        // TODO: send back a packet if we have one, otherwise: record that this subscriber is pulling, and pull from upstream
        if (buffer.size() == bufferPositions.get(receiver)) {
            // Finished the buffer TODO record that this subscriber is pulling, and pull from upstream
            setPulling(receiver);
            publishers.forEach(p -> p.pull(this));
        } else {
            send(receiver);
        }
    }

    @Override
    void finishPulling() {
        pullers.clear();
    }

    @Override
    void setPulling(Receiver<PACKET> receiver) {
        pullers.add(receiver);
    }

    @Override
    boolean isPulling() {
        return false;
    }

    private void send(Receiver<PACKET> receiver) {
        Integer pos = bufferPositions.get(receiver);
        receiver.receive(this, buffer.get(pos));
        bufferPositions.put(receiver, pos + 1);
    }

    @Override
    public void publishTo(Subscriber<PACKET> subscriber) {
        createBufferPosition(subscriber);
        super.publishTo(subscriber);
    }

    private void createBufferPosition(Receiver<PACKET> subscriber) {
        if (!subscribers.contains(subscriber)) {
            bufferPositions.put(subscriber, 0);
        }
    }
}
