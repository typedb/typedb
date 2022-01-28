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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;

import java.util.Stack;

public class BufferReactive<PACKET> extends ReactiveStreamBase<PACKET, PACKET> {

    private final SingleManager<PACKET> providerManager;
    private final Stack<PACKET> stack;

    protected BufferReactive(Publisher<PACKET> publisher, Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new Provider.SingleManager<>(publisher, this, monitor());
        this.stack = new Stack<>();
    }

    @Override
    protected Manager<PACKET> providerManager() {
        return providerManager;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        if (isPulling()) {
            finishPulling();
            subscriber().receive(this, packet);
        } else {
            // Can add an answer deduplication optimisation here, but means holding an extra set of all answers seen
            stack.add(packet);
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        if (!isPulling()) {
            if (stack.size() > 0) {
                receiver.receive(this, stack.pop());
            } else {
                setPulling();
                providerManager().pullAll();
            }
        }
    }
}
