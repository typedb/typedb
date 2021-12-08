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

package com.vaticle.typedb.core.reasoner.compute;

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

    protected <PACKET, PUB_CID, PUB_PID> void requestConnection(Driver<PROCESSOR> subscriberProcessor,
                                                                Subscriber<PACKET> subscriber,
                                                                PUB_CID publisherControllerId,
                                                                PUB_PID publisherProcessorId) {
        controller.execute(actor -> actor.findPublisherForConnection(new ConnectionRequest1<>(subscriberProcessor,
                                                                                              subscriber,
                                                                                              publisherControllerId,
                                                                                              publisherProcessorId)));
    }

    protected <SUB_PROCESSOR extends Processor<?, SUB_PROCESSOR>> void acceptConnection(
            Connection<OUTPUT, SUB_PROCESSOR, PROCESSOR> connection) {
        connection.subscribe(outlet());
        connection.subscriberProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected <PACKET, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> void finaliseConnection(
            Connection<PACKET, PROCESSOR, PUB_PROCESSOR> connection) {
        connection.subscriber().subscribe(connection);  // TODO: I think this isn't needed, the connection will already be pulling if it needs to be
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
        private final Subscriber<PACKET> subscriber;

        private Connection(Driver<PROCESSOR> subscriberProcessor, Driver<PUB_PROCESSOR> publisherProcessor,
                           Subscriber<PACKET> subscriber) {
            super(set(subscriber), set());
            this.subscriberProcessor = subscriberProcessor;
            this.publisherProcessor = publisherProcessor;
            this.subscriber = subscriber;
        }

        private Driver<PUB_PROCESSOR> publisherProcessor() {
            return publisherProcessor;
        }

        Driver<PROCESSOR> subscriberProcessor() {
            return subscriberProcessor;
        }

        Subscriber<PACKET> subscriber() {
            return subscriber;  // TODO: Duplicates subscribers()
        }

        @Override
        protected void publisherPull(Publisher<PACKET> publisher) {
            publisherProcessor().execute(actor -> publisher.pull(this));
        }

        @Override
        protected void subscriberReceive(Subscriber<PACKET> subscriber, PACKET packet) {
            subscriberProcessor().execute(actor -> subscriber.receive(this, packet));
        }

    }

    public static class ConnectionRequest1<PUB_CID, PUB_PID, PACKET, PROCESSOR extends Processor<?, PROCESSOR>> {

        private final Driver<PROCESSOR> subscriberProcessor;
        private final Subscriber<PACKET> subscriber;
        private final PUB_CID publisherControllerId;
        private final PUB_PID publisherProcessorId;

        protected ConnectionRequest1(Driver<PROCESSOR> subscriberProcessor, Subscriber<PACKET> subscriber,
                                     PUB_CID publisherControllerId, PUB_PID publisherProcessorId) {

            this.subscriberProcessor = subscriberProcessor;
            this.subscriber = subscriber;
            this.publisherControllerId = publisherControllerId;
            this.publisherProcessorId = publisherProcessorId;
        }

        public <PUB_CONTROLLER extends Controller<?, PUB_PID, PACKET, ?, PUB_CONTROLLER>> ConnectionRequest2<PUB_PID, PACKET,
                PROCESSOR, PUB_CONTROLLER> withPublisherController(Driver<PUB_CONTROLLER> publisherController, PUB_PID newPID, Subscriber<PACKET> newSubscriber) {
            return new ConnectionRequest2<>(subscriberProcessor, newPID, newSubscriber, publisherController);
        }

        public PUB_CID publisherControllerId() {
            return publisherControllerId;
        }

        public Subscriber<PACKET> subscriber() {
            return subscriber;
        }

        public PUB_PID publisherProcessorId() {
            return publisherProcessorId;
        }
    }

    public static class ConnectionRequest2<PUB_PID, PACKET, PROCESSOR extends Processor<?, PROCESSOR>, PUB_CONTROLLER extends Controller<?, PUB_PID, PACKET, ?, PUB_CONTROLLER>> {

        private final Driver<PROCESSOR> subscriberProcessor;
        private final PUB_PID publisherProcessorId;
        private final Subscriber<PACKET> subscriber;
        private final Driver<PUB_CONTROLLER> publisherController;

        protected ConnectionRequest2(Driver<PROCESSOR> subscriberProcessor, PUB_PID publisherProcessorId,
                                     Subscriber<PACKET> subscriber, Driver<PUB_CONTROLLER> publisherController) {

            this.subscriberProcessor = subscriberProcessor;
            this.publisherProcessorId = publisherProcessorId;
            this.subscriber = subscriber;
            this.publisherController = publisherController;
        }

        public Driver<PUB_CONTROLLER> publisherController() {
            return publisherController;
        }

        public <PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> Connection<PACKET, PROCESSOR, PUB_PROCESSOR> addPublisherProcessor(Driver<PUB_PROCESSOR> publisherProcessor) {
            return new Connection<>(subscriberProcessor, publisherProcessor, subscriber);
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
