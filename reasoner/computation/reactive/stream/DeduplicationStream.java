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

package com.vaticle.typedb.core.reasoner.computation.reactive.stream;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor.TerminationTracker;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;

import java.util.HashSet;
import java.util.Set;

public class DeduplicationStream<PACKET> extends SingleReceiverStream<PACKET, PACKET> {

    private final ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry;
    private final Set<PACKET> deduplicationSet;

    public DeduplicationStream(Publisher<PACKET> publisher, TerminationTracker monitor, String groupName) {
        super(monitor, groupName);
        this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(publisher, this);
        this.deduplicationSet = new HashSet<>();
    }

    @Override
    protected ProviderRegistry<PACKET> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        if (deduplicationSet.add(packet)) {
            receiverRegistry().recordReceive();
            receiverRegistry().receiver().receive(this, packet);
        } else {
            if (receiverRegistry().isPulling()) providerRegistry.pull(provider);  // Automatic retry
            tracker().syncAndReportAnswerDestroy(this, receiverRegistry().monitors());  // Already seen this answer, so join this path
        }
    }
}
