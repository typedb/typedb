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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;

import java.util.function.Function;

public class FlatMapStream<INPUT, OUTPUT> extends SingleReceiverStream<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;
    private final ProviderRegistry.SingleProviderRegistry<INPUT> providerRegistry;

    public FlatMapStream(Publisher<INPUT> publisher, Function<INPUT, FunctionalIterator<OUTPUT>> transform,
                         Actor.Driver<Monitor> monitor, String groupName) {
        super(monitor, groupName);
        this.transform = transform;
        this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(publisher, this, monitor);
    }

    @Override
    protected ProviderRegistry.SingleProviderRegistry<INPUT> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        super.receive(provider, packet);
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        if (transformed.hasNext()) {
            receiverRegistry().setNotPulling();
            // This can actually create more receive() calls to downstream than the number of pulls it receives. Protect against by manually adding .buffer() after calls to flatMap
            transformed.forEachRemaining(t -> {
                monitor().execute(actor -> actor.createAnswer(this));
                receiverRegistry().receiver().receive(this, t);
            });
        } else {
            if (receiverRegistry().isPulling()) providerRegistry().retry(provider);
        }
        monitor().execute(actor -> actor.consumeAnswer(this));
    }
}
