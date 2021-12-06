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

package com.vaticle.typedb.core.reasoner.reactiveFramework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.reactive.IdentityReactive;
import com.vaticle.typedb.core.reasoner.reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Subscriber;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<?, ?, OUTPUT, PROCESSOR, ?>> controller;
    private final Outlet<OUTPUT> outlet;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<? extends Controller<?, ?, OUTPUT, PROCESSOR, ?>> controller,
                        String name, Outlet<OUTPUT> outlet) {
        super(driver, name);
        this.controller = controller;
        this.outlet = outlet;
    }

    public Outlet<OUTPUT> outlet() {
        return outlet;
    }

    @Override
    protected void exception(Throwable e) {}

    protected <PACKET, PUB_CID, PUB_PID, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>>
    void requestConnection(Connection.Builder<PACKET, PROCESSOR, PUB_CID, PUB_PID, PUB_PROCESSOR> connectionBuilder) {
        controller.execute(actor -> actor.findPublishingConnection(connectionBuilder));
    }

    protected <SUB_PROCESSOR extends Processor<?, SUB_PROCESSOR>>
    void acceptConnection(Connection<OUTPUT, SUB_PROCESSOR, PROCESSOR> connection) {
        connection.subscriberProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected <PACKET, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>>
    void finaliseConnection(Connection<PACKET, PROCESSOR, PUB_PROCESSOR> connection) {
        connection.subscriber().subscribe(connection);
    }

    public static abstract class Outlet<OUTPUT> extends IdentityReactive<OUTPUT> {

        Outlet() {
            super(set(), set());
        }

        public static class Single<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void publish(Subscriber<OUTPUT> subscriber) {
                if (subscribers().size() > 0) throw TypeDBException.of(ILLEGAL_STATE);
                else subscribers.add(subscriber);
            }

        }

        public static class DynamicMulti<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void publish(Subscriber<OUTPUT> subscriber) {
                super.publish(subscriber);  // TODO: This needs to record the subscribers read position in the buffer and maintain it.
            }

        }

    }

    public static class Connection<PACKET, PROCESSOR extends Processor<?, PROCESSOR>,
            PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> extends IdentityReactive<PACKET> {

        private final Driver<PROCESSOR> subscriberProcessor;
        private final Driver<PUB_PROCESSOR> publisherProcessor;
        private final Subscriber<PACKET> inletPort;

        public Connection(Driver<PROCESSOR> subscriberProcessor, Driver<PUB_PROCESSOR> publisherProcessor,
                          Subscriber<PACKET> subscriber) {
            super(set(subscriber), set());
            this.subscriberProcessor = subscriberProcessor;
            this.publisherProcessor = publisherProcessor;
            this.inletPort = subscriber;
        }

        private Driver<PUB_PROCESSOR> publisherProcessor() {
            return publisherProcessor;
        }

        Driver<PROCESSOR> subscriberProcessor() {
            return subscriberProcessor;
        }

        Subscriber<PACKET> subscriber() {
            return inletPort;  // TODO: Duplicates subscribers()
        }

        @Override
        protected void publisherPull(Publisher<PACKET> publisher) {
            publisherProcessor().execute(actor -> publisher.pull(this));
        }

        @Override
        protected void subscriberReceive(Subscriber<PACKET> subscriber, PACKET packet) {
            subscriberProcessor().execute(actor -> subscriber.receive(this, packet));
        }

        public static class Builder<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, PUB_CID, PUB_PID,
                PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> {

            private final Driver<PROCESSOR> subscriberProcessor;
            private final PUB_CID publisherControllerId;
            private Driver<PUB_PROCESSOR> publisherProcessor;
            private final PUB_PID publisherProcessorId;
            private final Subscriber<PACKET> subscriber;

            protected Builder(Driver<PROCESSOR> subscriberProcessor, PUB_CID publisherControllerId,
                              PUB_PID publisherProcessorId, Subscriber<PACKET> subscriber) {
                this.subscriberProcessor = subscriberProcessor;
                this.publisherControllerId = publisherControllerId;
                this.publisherProcessorId = publisherProcessorId;
                this.subscriber = subscriber;
            }

            PUB_CID publisherControllerId() {
                return publisherControllerId;
            }

            protected Builder<PACKET, PROCESSOR, PUB_CID, PUB_PID, PUB_PROCESSOR> publisherProcessor(Driver<PUB_PROCESSOR> publisherProcessor) {
                assert this.publisherProcessor == null;
                this.publisherProcessor = publisherProcessor;
                return this;
            }

            Connection<PACKET, PROCESSOR, PUB_PROCESSOR> build() {
                assert subscriberProcessor != null;
                assert publisherProcessor != null;
                assert subscriber != null;
                return new Connection<>(subscriberProcessor, publisherProcessor, subscriber);
            }

            public PUB_PID publisherProcessorId() {
                return publisherProcessorId;
            }
        }
    }

    public static class Source<INPUT> implements Publisher<INPUT> {

        private final Supplier<FunctionalIterator<INPUT>> iteratorSupplier;
        private final Map<Subscriber<INPUT>, FunctionalIterator<INPUT>> iterators;

        public Source(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
            this.iterators = new HashMap<>();
        }

        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            return new Source<>(iteratorSupplier);
        }

        @Override
        public void publish(Subscriber<INPUT> subscriber) {
            // subscribers only need to be dynamically recorded on pull()
        }

        @Override
        public void pull(Subscriber<INPUT> subscriber) {
            FunctionalIterator<INPUT> iterator = iterators.computeIfAbsent(subscriber, s -> iteratorSupplier.get());
            if (iterator.hasNext()) subscriber.receive(this, iterator.next());
        }
    }
}
