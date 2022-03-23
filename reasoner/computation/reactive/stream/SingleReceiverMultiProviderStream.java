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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public class SingleReceiverMultiProviderStream<INPUT, OUTPUT> extends SingleReceiverStream<INPUT, OUTPUT> {

    private final ProviderRegistry.Multi<Provider.Sync<INPUT>> providerRegistry;

    protected SingleReceiverMultiProviderStream(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.providerRegistry = new ProviderRegistry.Multi<>(this, processor);
    }

    @Override
    protected ProviderRegistry.Multi<Provider.Sync<INPUT>> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void pull(Receiver.Sync<OUTPUT> receiver) {
        assert receiver.equals(receiverRegistry().receiver());
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver.identifier(), identifier()));
        receiverRegistry().recordPull(receiver);
        providerRegistry().nonPulling().forEach(p -> p.pull(this));
    }
}
