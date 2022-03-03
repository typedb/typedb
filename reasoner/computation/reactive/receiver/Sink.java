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

package com.vaticle.typedb.core.reasoner.computation.reactive.receiver;

import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public abstract class Sink<PACKET> implements Subscriber<PACKET> {

    private final ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry;
    private Monitor.MonitorRef monitor;

    protected Sink() {
        this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this, monitor);
    }

    protected ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry() {
        return providerRegistry;
    }

    protected Monitor.MonitorRef monitor() {
        return monitor;
    }

    public void setMonitor(Monitor.MonitorRef monitor) {
        this.monitor = monitor;
    }

    @Override
    public void subscribeTo(Provider<PACKET> provider) {
        providerRegistry().add(provider);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        providerRegistry().recordReceive(provider);
    }

}
