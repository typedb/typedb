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

package com.vaticle.typedb.core.reasoner.stream.reactive;

import java.util.Set;

public class FindFirstReactive<T> extends IdentityReactive<T> {

    private boolean packetFound;

    FindFirstReactive(Set<Receiver<T>> downstreams, Set<Pullable<T>> upstreams) {
        super(downstreams, upstreams);
        this.packetFound = false;
    }

    @Override
    public void receive(Pullable<T> upstream, T packet) {  // TODO: Doesn't do a retry
        packetFound = true;
        super.receive(upstream, packet);
    }

    @Override
    public void pull(Receiver<T> receiver) {
        if (!packetFound) super.pull(receiver);
    }
}
