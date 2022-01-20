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

import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.Set;

public abstract class ReactiveBase<INPUT, OUTPUT> extends ReactiveImpl<INPUT, OUTPUT> {

    protected Receiver<OUTPUT> subscriber;
    private boolean isPulling;

    protected ReactiveBase(Set<Publisher<INPUT>> publishers, PacketMonitor monitor, String groupName) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        super(publishers, monitor, groupName);
        this.isPulling = false;
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        if (!isPulling()) {
            setPulling();
            pullFromAllPublishers();
        }
    }

    public void finishPulling() {
        isPulling = false;
    }

    void setPulling() {
        isPulling = true;
    }

    @Override
    protected boolean isPulling() {
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

    public void sendTo(Receiver<OUTPUT> receiver) {
        // Allows sending of data without the downstream being able to pull from here
        setSubscriber(receiver);
    }

    public Receiver<OUTPUT> subscriber() {
        assert this.subscriber != null;
        return subscriber;
    }

}
