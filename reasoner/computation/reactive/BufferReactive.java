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

import java.util.Stack;

public class BufferReactive<PACKET> extends ReactiveBase<PACKET, PACKET> {

    private final SingleManager<PACKET> providerManager;
    final Stack<PACKET> buffer;

    protected BufferReactive(Publisher<PACKET> publisher, PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new Provider.SingleManager<>(publisher, this);
        this.buffer = new Stack<>();
    }

    @Override
    protected Manager<PACKET> providerManager() {
        return providerManager;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        if (isPulling()) {
            subscriber().receive(this, packet);
        } else {
            finishPulling();
            if (!buffer.add(packet)) {
                monitor().onPathJoin();
            }
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        if (buffer.size() > 0) {
            receiver.receive(this, buffer.pop());
        } else if (!isPulling()) {
            setPulling();
            providerManager().pullAll();
        }
    }
}
