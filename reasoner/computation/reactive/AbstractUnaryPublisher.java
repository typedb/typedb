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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;

public abstract class AbstractUnaryPublisher<OUTPUT> extends AbstractPublisher<OUTPUT> {

    protected SingleReceiverRegistry<OUTPUT> receiverRegistry;

    protected AbstractUnaryPublisher(Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.receiverRegistry = new SingleReceiverRegistry<>();
    }

    @Override
    protected SingleReceiverRegistry<OUTPUT> receiverRegistry() {
        return receiverRegistry;
    }

    @Override
    public void publishTo(Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.subscribeTo(this);
    }

}
