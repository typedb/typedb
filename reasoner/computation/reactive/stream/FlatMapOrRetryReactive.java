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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;

import java.util.function.Function;

public class FlatMapOrRetryReactive<INPUT, OUTPUT> extends AbstractSingleReceiverReactiveStream<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;
    private final SingleProviderRegistry<INPUT> providerManager;

    public FlatMapOrRetryReactive(Publisher<INPUT> publisher, Function<INPUT, FunctionalIterator<OUTPUT>> transform,
                                  Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.transform = transform;
        this.providerManager = new SingleProviderRegistry<>(publisher, this);
    }

    @Override
    protected ProviderRegistry<INPUT> providerRegistry() {
        return providerManager;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        super.receive(provider, packet);
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        if (transformed.hasNext()) {
            receiverRegistry().recordReceive();
            // TODO: This can actually create more receive() calls to downstream than the number of pull()s it receives. Should buffer instead. Protected against by manually adding .buffer() after calls to flatMap
            transformed.forEachRemaining(t -> {
                monitor().onAnswerCreate(this);
                receiverRegistry().receiver().receive(this, t);
            });
        } else if (receiverRegistry().isPulling()) {
            providerManager.pull(provider);  // Automatic retry
        }
        monitor().onAnswerDestroy(this);  // Because we discarded the original packet and gained as many as are in the iterator
    }
}
