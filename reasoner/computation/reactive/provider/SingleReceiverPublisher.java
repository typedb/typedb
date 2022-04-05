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

package com.vaticle.typedb.core.reasoner.computation.reactive.provider;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

public abstract class SingleReceiverPublisher<OUTPUT> extends AbstractPublisher<OUTPUT> {

    protected ReceiverRegistry.Single<Subscriber<OUTPUT>> receiverRegistry;

    protected SingleReceiverPublisher(Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.receiverRegistry = new ReceiverRegistry.Single<>();
    }

    @Override
    protected ReceiverRegistry.Single<Subscriber<OUTPUT>> receiverRegistry() {
        return receiverRegistry;
    }

    @Override
    public void registerReceiver(Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.registerProvider(this);
    }

}
