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

public class FindFirstReactive<PACKET> extends AbstractUnaryReactiveStream<PACKET, PACKET> {

    private final SingleProviderRegistry<PACKET> providerManager;
    private boolean packetFound;

    FindFirstReactive(Publisher<PACKET> publisher, Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new SingleProviderRegistry<>(publisher, this);
        this.packetFound = false;
    }

    @Override
    protected ProviderRegistry<PACKET> providerRegistry() {
        return providerManager;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        if (!packetFound) {
            packetFound = true;
            receiverRegistry().recordReceive();
            receiverRegistry().receiver().receive(this, packet);
        } else {
            monitor().onAnswerDestroy(this);
        }
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        // TODO: THis is the only unary reactive that overrides pull()
        if (!packetFound) super.pull(receiver);  // TODO: Could this cause a failure to terminate if multiple upstream paths are never joined?
    }
}
