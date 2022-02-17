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
import com.vaticle.typedb.core.reasoner.utils.Tracer;

public abstract class AbstractUnaryReactiveStream<INPUT, OUTPUT> extends AbstractReactiveStream<INPUT, OUTPUT> {

    protected Receiver<OUTPUT> subscriber;
    private boolean isPulling;

    protected AbstractUnaryReactiveStream(Monitoring monitor, String groupName) {  // TODO: Do we need to initialise with publishers or should we always add dynamically?
        super(monitor, groupName);
        this.isPulling = false;
    }

    @Override
    protected abstract ProviderRegistry<INPUT> providerManager();

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet, monitor().count()));
        providerManager().receivedFrom(provider);
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        assert receiver.equals(subscriber);  // TODO: Make a proper exception for this
        if (!isPulling()) {
            setPulling();
            providerManager().pullAll();
        }
    }

    @Override
    public void subscribeTo(Provider<INPUT> provider) {
        providerManager().add(provider);
        if (isPulling()) providerManager().pull(provider);
    }

    @Override
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
