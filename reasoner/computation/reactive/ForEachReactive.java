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

import java.util.function.Consumer;

public class ForEachReactive<PACKET> implements Receiver.Subscriber<PACKET> {

    private final Consumer<PACKET> consumer;
    private final String groupName;
    private Provider<PACKET> publisher;

    protected ForEachReactive(Consumer<PACKET> consumer, String groupName) {
        this.consumer = consumer;
        this.groupName = groupName;
    }

    @Override
    public void subscribeTo(Provider<PACKET> publisher) {
        assert this.publisher == null;
        this.publisher = publisher;
        publisher.pull(this);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        consumer.accept(packet);
    }

    @Override
    public String groupName() {
        return groupName;
    }
}
