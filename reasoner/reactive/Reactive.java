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

package com.vaticle.typedb.core.reasoner.reactive;

import java.util.Set;

public abstract class Reactive<INPUT, OUTPUT> extends ChainablePublisher<OUTPUT> implements Subscriber.Subscribing<INPUT> {

    private final Set<Publisher<INPUT>> publishers;
    protected boolean isPulling;

    protected Reactive(Set<Publisher<INPUT>> publishers) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        this.publishers = publishers;
        this.isPulling = false;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        subscribers.add(subscriber);
        if (!isPulling) {
            publishers.forEach(p -> p.pull(this));
            isPulling = true;
        }
    }

    @Override
    public void subscribeTo(Publisher<INPUT> publisher) {
        publishers.add(publisher);  // Will fail if publishers is an immutable set TODO: How should we best constrain whether more than one publisher is permitted?
        if (isPulling) publisher.pull(this);
    }

}
