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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Subscriber;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public abstract class Sink<PACKET> implements Subscriber<PACKET> { // TODO: Collapse into RootSink

    private final ProviderRegistry.Single<Publisher<PACKET>> providerRegistry;
    private final Processor<?, ?, ?, ?> processor;

    protected Sink(Processor<?, ?, ?, ?> processor) {
        this.providerRegistry = new ProviderRegistry.Single<>();
        this.processor = processor;
    }

    protected ProviderRegistry.Single<Publisher<PACKET>> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public Processor<?, ?, ?, ?> processor() {
        return processor;
    }

    @Override
    public void registerProvider(Publisher<PACKET> provider) {
        if (providerRegistry().add(provider)) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
        }
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), identifier(), packet));
        providerRegistry().recordReceive(publisher);
    }

}
