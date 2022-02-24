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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;

public class FanInReactive<PACKET> extends AbstractSingleReceiverReactiveStream<PACKET, PACKET> {

    private final MultiProviderRegistry<PACKET> providerManager;

    protected FanInReactive(Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new MultiProviderRegistry<>(this);
    }

    @Override
    protected MultiProviderRegistry<PACKET> providerRegistry() {
        return providerManager;
    }

    public static <T> FanInReactive<T> fanIn(Monitoring monitor, String groupName) {
        return new FanInReactive<>(monitor, groupName);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        receiverRegistry().recordReceive();
        receiverRegistry().receiver().receive(this, packet);
    }

    public void finaliseProviders() {
        assert monitor() != null;
        final int numForks = providerRegistry().size() - 1;
        if (numForks > 0) monitor().onPathFork(numForks, this);
    }
}
