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

public class FanInReactive<PACKET> extends ReactiveBase<PACKET, PACKET> {

    private final Provider.MultiManager<PACKET> providerManager;

    protected FanInReactive(PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
        this.providerManager = new Provider.MultiManager<>(this, monitor);
    }

    @Override
    protected Manager<PACKET> providerManager() {
        return providerManager;
    }

    public static <T> FanInReactive<T> fanIn(PacketMonitor monitor, String groupName) {
        return new FanInReactive<>(monitor, groupName);
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        if (!isPulling()) {
            setPulling();
            providerManager().pullAll();
        }
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        super.receive(provider, packet);
        finishPulling();
        subscriber().receive(this, packet);
    }
}
