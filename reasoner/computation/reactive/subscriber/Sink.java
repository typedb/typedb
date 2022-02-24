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

package com.vaticle.typedb.core.reasoner.computation.reactive.subscriber;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public abstract class Sink<PACKET> implements Subscriber<PACKET> {

    private final ProviderRegistry.SingleProviderRegistry<PACKET> providerManager;
    private Monitoring monitor;

    protected Sink() {
        this.providerManager = new ProviderRegistry.SingleProviderRegistry<>(this);
    }

    protected ProviderRegistry.SingleProviderRegistry<PACKET> providerManager() {
        return providerManager;
    }

    protected Monitoring monitor() {
        return monitor;
    }

    public void setMonitor(Monitoring monitor) {
        this.monitor = monitor;
    }

    @Override
    public void subscribeTo(Provider<PACKET> provider) {
        providerManager().add(provider);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerManager().recordReceive(provider);
    }

    public void pull() {
        providerManager().pullAll();
    }
}
