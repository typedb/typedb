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
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
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
        controller.execute(actor -> actor.findUpstreamConnection(connectionBuilder));
    }

    protected <SUB_PROCESSOR extends Processor<?, SUB_PROCESSOR>>
    void acceptConnection(Connection<OUTPUT, SUB_PROCESSOR, PROCESSOR> connection) {
        connection.downstreamProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected <PACKET, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>>
    void finaliseConnection(Connection<PACKET, PROCESSOR, PUB_PROCESSOR> connection) {
        connection.subscriber().addPublisher(connection);
    }

    public static abstract class Outlet<OUTPUT> extends IdentityReactive<OUTPUT> {

        Outlet() {
            super(set(), set());
        }

        public static class Single<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void addSubscriber(Subscriber<OUTPUT> subscriber) {
                if (subscribers().size() > 0) throw TypeDBException.of(ILLEGAL_STATE);
                else subscribers.add(subscriber);
            }

        }

        public static class DynamicMulti<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void addSubscriber(Subscriber<OUTPUT> subscriber) {
                super.addSubscriber(subscriber);  // TODO: This needs to record the downstreams read position in the buffer and maintain it.
            }

        }

    }

    public static class Connection<PACKET, PROCESSOR extends Processor<?, PROCESSOR>,
            PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> extends IdentityReactive<PACKET> {

        private final Driver<PROCESSOR> downstreamProcessor;
        private final Driver<PUB_PROCESSOR> upstreamProcessor;
        private final Subscriber<PACKET> inletPort;

        public Connection(Driver<PROCESSOR> downstreamProcessor, Driver<PUB_PROCESSOR> upstreamProcessor,
                          Subscriber<PACKET> subscriber) {
            super(set(subscriber), set());
            this.downstreamProcessor = downstreamProcessor;
            this.upstreamProcessor = upstreamProcessor;
            this.inletPort = subscriber;
        }

        private Driver<PUB_PROCESSOR> upstreamProcessor() {
            return upstreamProcessor;
        }

        Driver<PROCESSOR> downstreamProcessor() {
            return downstreamProcessor;
        }

        Subscriber<PACKET> subscriber() {
            return inletPort;  // TODO: Duplicates downstreams()
        }

        @Override
        protected void publisherPull(Publisher<PACKET> publisher) {
            upstreamProcessor().execute(actor -> publisher.pull(this));
        }

        @Override
        protected void subscriberReceive(Subscriber<PACKET> subscriber, PACKET packet) {
            downstreamProcessor().execute(actor -> subscriber.receive(this, packet));
        }

        public static class Builder<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, PUB_CID, PUB_PID,
                PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>> {

            private final Driver<PROCESSOR> downstreamProcessor;
            private final PUB_CID upstreamControllerId;
            private Driver<PUB_PROCESSOR> upstreamProcessor;
            private final PUB_PID upstreamProcessorId;
            private final Reactive<PACKET, ?> inletPort;

            protected Builder(Driver<PROCESSOR> downstreamProcessor, PUB_CID upstreamControllerId,
                              PUB_PID upstreamProcessorId, Reactive<PACKET, ?> inletPort) {
                this.downstreamProcessor = downstreamProcessor;
                this.upstreamControllerId = upstreamControllerId;
                this.upstreamProcessorId = upstreamProcessorId;
                this.inletPort = inletPort;
            }

            PUB_CID upstreamControllerId() {
                return upstreamControllerId;
            }

            protected Builder<PACKET, PROCESSOR, PUB_CID, PUB_PID, PUB_PROCESSOR> addUpstreamProcessor(Driver<PUB_PROCESSOR> upstreamProcessor) {
                assert this.upstreamProcessor == null;
                this.upstreamProcessor = upstreamProcessor;
                return this;
            }

            Connection<PACKET, PROCESSOR, PUB_PROCESSOR> build() {
                assert downstreamProcessor != null;
                assert upstreamProcessor != null;
                assert inletPort != null;
                return new Connection<>(downstreamProcessor, upstreamProcessor, inletPort);
            }

            public PUB_PID upstreamProcessorId() {
                return upstreamProcessorId;
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
        public void addSubscriber(Subscriber<INPUT> subscriber) {
            // subscribers only need to be dynamically recorded on pull()
        }

        @Override
        public void pull(Subscriber<INPUT> subscriber) {
            FunctionalIterator<INPUT> iterator = iterators.computeIfAbsent(subscriber, s -> iteratorSupplier.get());
            if (iterator.hasNext()) subscriber.receive(this, iterator.next());
        }
    }
}
