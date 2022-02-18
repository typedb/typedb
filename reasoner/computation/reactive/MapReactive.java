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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;

import java.util.function.Function;

public class MapReactive<INPUT, OUTPUT> extends AbstractUnaryReactiveStream<INPUT, OUTPUT> {

    private final Function<INPUT, OUTPUT> mappingFunc;
    private final SingleProviderRegistry<INPUT> providerManager;

    protected MapReactive(Publisher<INPUT> publisher, Function<INPUT, OUTPUT> mappingFunc, Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.mappingFunc = mappingFunc;
        this.providerManager = new SingleProviderRegistry<>(publisher, this);
    }

    @Override
    protected ProviderRegistry<INPUT> providerRegistry() {
        return providerManager;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        super.receive(provider, packet);
        receiverRegistry().recordReceive();
        receiverRegistry().receiver().receive(this, mappingFunc.apply(packet));
    }
}
