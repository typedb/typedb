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

import java.util.Set;

public class IdentityReactive<PACKET> extends ReactiveImpl<PACKET, PACKET> {

    protected IdentityReactive(Set<Subscriber<PACKET>> subscribers, Set<Publisher<PACKET>> publishers) {
        super(subscribers, publishers);
    }

    public static <T>  IdentityReactive<T> noOp(Set<Subscriber<T>> subscribers, Set<Publisher<T>> publishers) {
        return new IdentityReactive<>(subscribers, publishers);
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        subscribers().forEach(subscriber -> subscriberReceive(subscriber, packet));
    }
}
