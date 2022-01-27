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

import java.util.HashSet;
import java.util.Set;

public class DeduplicationReactive<PACKET> extends ReactiveStreamBase<PACKET, PACKET> {

    private final SingleManager<PACKET> providerManager;
    private final Set<PACKET> deduplicationSet;

    protected DeduplicationReactive(Publisher<PACKET> publisher, PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new Provider.SingleManager<>(publisher, this, monitor());
        this.deduplicationSet = new HashSet<>();
    }

    @Override
    protected Manager<PACKET> providerManager() {
        return providerManager;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        if (deduplicationSet.add(packet)) {
            finishPulling();
            subscriber().receive(this, packet);
        } else {
            if (isPulling()) providerManager.pull(provider);  // Automatic retry
            monitor().onPathJoin(this);  // Already seen this answer, so join this path
        }
    }
}
