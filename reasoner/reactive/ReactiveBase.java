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

import java.util.HashSet;
import java.util.Set;

public abstract class ReactiveBase<INPUT, OUTPUT> extends ReactiveImpl<INPUT, OUTPUT> {

    protected final Set<Provider<INPUT>> publishers;
    protected Receiver<OUTPUT> subscriber;
    private boolean isPulling;

    protected ReactiveBase(Set<Provider.Publisher<INPUT>> publishers) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        this.publishers = new HashSet<>();
        publishers.forEach(pub -> pub.publishTo(this));
        this.isPulling = false;
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        if (!isPulling()) {
            publishers.forEach(p -> p.pull(this));
            setPulling();
        }
    }

    @Override
    public void subscribeTo(Provider<INPUT> publisher) {
        publishers.add(publisher);
        if (isPulling()) publisher.pull(this);
    }

    protected void finishPulling() {
        isPulling = false;
    }

    void setPulling() {
        isPulling = true;
    }

    boolean isPulling() {
        return isPulling;
    }

    private void setSubscriber(Receiver<OUTPUT> subscriber) {
        assert this.subscriber == null;
        this.subscriber = subscriber;
    }

    @Override
    public void publishTo(Subscriber<OUTPUT> subscriber) {
        setSubscriber(subscriber);
        subscriber.subscribeTo(this);
    }

    protected Receiver<OUTPUT> subscriber() {
        assert this.subscriber != null;
        return subscriber;
    }

}
