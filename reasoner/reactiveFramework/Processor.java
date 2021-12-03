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

    protected <PACKET, UPS_CID, UPS_PID, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>>
    void requestConnection(Connection.Builder<PACKET, PROCESSOR, UPS_CID, UPS_PID, UPS_PROCESSOR> connectionBuilder) {
        controller.execute(actor -> actor.findUpstreamConnection(connectionBuilder));
    }

    protected <DNS_PROCESSOR extends Processor<?, DNS_PROCESSOR>>
    void buildConnection(Connection.Builder<OUTPUT, DNS_PROCESSOR, ?, ?, PROCESSOR> connectionBuilder) {
        Connection<OUTPUT, DNS_PROCESSOR, PROCESSOR> connection = connectionBuilder.addUpstreamProcessor(driver()).build();  // TODO: The connection could already have been built by the controller
        outlet().addSubscriber(connection);
        connection.downstreamProcessor().execute(actor -> actor.setReady(connection));
    }

    protected <PACKET, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> void setReady(Connection<PACKET, PROCESSOR, UPS_PROCESSOR> connection) {
        connection.inletPort().addPublisher(connection);
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

    public static class Connection<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> extends IdentityReactive<PACKET> {

        private final Driver<PROCESSOR> downstreamProcessor;
        private final Driver<UPS_PROCESSOR> upstreamProcessor;
        private final Reactive<PACKET, ?> inletPort;

        public Connection(Driver<PROCESSOR> downstreamProcessor, Driver<UPS_PROCESSOR> upstreamProcessor, Reactive<PACKET, ?> inletPort) {
            super(set(inletPort), set());
            this.downstreamProcessor = downstreamProcessor;
            this.upstreamProcessor = upstreamProcessor;
            this.inletPort = inletPort;
        }

        private Driver<UPS_PROCESSOR> upstreamProcessor() {
            return upstreamProcessor;
        }

        Driver<PROCESSOR> downstreamProcessor() {
            return downstreamProcessor;
        }

        Reactive<PACKET, ?> inletPort() {
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

        public static class Builder<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, UPS_CID, UPS_PID, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> {

            private final Driver<PROCESSOR> downstreamProcessor;
            private final UPS_CID upstreamControllerId;
            private Driver<UPS_PROCESSOR> upstreamProcessor;
            private final UPS_PID upstreamProcessorId;
            private final Reactive<PACKET, ?> inletPort;

            protected Builder(Driver<PROCESSOR> downstreamProcessor, UPS_CID upstreamControllerId,
                              UPS_PID upstreamProcessorId, Reactive<PACKET, ?> inletPort) {
                this.downstreamProcessor = downstreamProcessor;
                this.upstreamControllerId = upstreamControllerId;
                this.upstreamProcessorId = upstreamProcessorId;
                this.inletPort = inletPort;
            }

            UPS_CID upstreamControllerId() {
                return upstreamControllerId;
            }

            protected Builder<PACKET, PROCESSOR, UPS_CID, UPS_PID, UPS_PROCESSOR> addUpstreamProcessor(Driver<UPS_PROCESSOR> upstreamProcessor) {
                assert this.upstreamProcessor == null;
                this.upstreamProcessor = upstreamProcessor;
                return this;
            }

            Connection<PACKET, PROCESSOR, UPS_PROCESSOR> build() {
                assert downstreamProcessor != null;
                assert upstreamProcessor != null;
                assert inletPort != null;
                return new Connection<>(downstreamProcessor, upstreamProcessor, inletPort);
            }

            public UPS_PID upstreamProcessorId() {
                return upstreamProcessorId;
            }
        }
    }

    public static class Source<INPUT> implements Publisher<INPUT> {

        private final Supplier<FunctionalIterator<INPUT>> iteratorSupplier;
        private FunctionalIterator<INPUT> iterator;

        public Source(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
            this.iterator = null;
        }

        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            return new Source<>(iteratorSupplier);
        }

        @Override
        public void pull(Subscriber<INPUT> subscriber) {
            if (iterator == null) iterator = iteratorSupplier.get();
            if (iterator.hasNext()) {
                subscriber.receive(this, iterator.next());
            }
        }
    }
}
