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

public class FanInStream<PACKET> extends SingleReceiverMultiProviderStream<PACKET, PACKET> {

    private final ProviderRegistry.Multi<Publisher<PACKET>> providerRegistry;

    protected FanInStream(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.providerRegistry = new ProviderRegistry.Multi<>(this, processor);
    }

    @Override
    protected ProviderRegistry.Multi<Publisher<PACKET>> providerRegistry() {
        return providerRegistry;
    }

    public static <T> FanInStream<T> fanIn(Processor<?, ?, ?, ?> processor) {
        return new FanInStream<>(processor);
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        super.receive(publisher, packet);
        receiverRegistry().setNotPulling();
        receiverRegistry().receiver().receive(this, packet);
    }

    public void finaliseProviders() {
        assert processor().monitor() != null;
        final int numForks = providerRegistry().size() - 1;
        if (numForks > 0) processor().monitor().execute(actor -> actor.forkFrontier(numForks, identifier()));
    }
}
