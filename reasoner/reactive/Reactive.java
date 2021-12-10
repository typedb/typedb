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

import com.vaticle.typedb.core.reasoner.reactive.Receiver.Subscriber;

import java.util.Set;

public abstract class Reactive<INPUT, OUTPUT> extends PublisherImpl<OUTPUT> implements Subscriber<INPUT> {

    protected final Set<Provider<INPUT>> publishers;

    protected Reactive(Set<Provider<INPUT>> publishers) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        this.publishers = publishers;
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        subscribers.add(receiver);
        if (!isPulling()) {
            publishers.forEach(p -> p.pull(this));
            setPulling(receiver);
        }
    }

    @Override
    public void subscribeTo(Provider<INPUT> publisher) {
        publishers.add(publisher);  // Will fail if publishers is an immutable set TODO: How should we best constrain whether more than one publisher is permitted?
        if (isPulling()) publisher.pull(this);
    }

    abstract void finishPulling();

    abstract void setPulling(Receiver<OUTPUT> receiver);

    abstract boolean isPulling();

}
