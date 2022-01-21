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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.function.Function;

public class FlatMapOrRetryReactive<INPUT, OUTPUT> extends ReactiveBase<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;
    private final SingleManager<INPUT> providerManager;

    FlatMapOrRetryReactive(Publisher<INPUT> publisher, Function<INPUT, FunctionalIterator<OUTPUT>> transform,
                           PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
        this.transform = transform;
        this.providerManager = new Provider.SingleManager<>(publisher, this);
    }

    @Override
    protected Manager<INPUT> providerManager() {
        return providerManager;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        super.receive(provider, packet);
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        if (transformed.hasNext()) {
            finishPulling();
            // TODO: This can actually create more receive() calls to downstream than the number of pull()s it receives. Should buffer instead.
            transformed.forEachRemaining(t -> {
                monitor().onPathFork(1);
                subscriber().receive(this, t);
            });
        } else if (isPulling()) {
            providerManager.pull(provider);  // Automatic retry
        }
        monitor().onPathJoin();  // Because we discarded the original packet and gained as many as are in the iterator
    }
}
