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

package com.vaticle.typedb.core.reasoner.stream;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.poller.Poller;
import com.vaticle.typedb.core.common.poller.Pollers;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<?, ?, OUTPUT, PROCESSOR, ?>> controller;
    private final OutletManager<OUTPUT> outletManager;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<? extends Controller<?, ?, OUTPUT, PROCESSOR, ?>> controller,
                        String name, OutletManager<OUTPUT> outletManager) {
        super(driver, name);
        this.controller = controller;
        this.outletManager = outletManager;
    }

    public OutletManager<OUTPUT> outletManager() {
        return outletManager;
    }

    @Override
    protected void exception(Throwable e) {}

    protected <PACKET, UPS_CID, UPS_PID, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>>
    void requestConnection(Connection.Builder<PACKET, PROCESSOR, UPS_CID, UPS_PID, UPS_PROCESSOR> connectionBuilder) {
        controller.execute(actor -> actor.findUpstreamConnection(connectionBuilder));
    }

    protected <DNS_PROCESSOR extends Processor<?, DNS_PROCESSOR>>
    void buildConnection(Connection.Builder<OUTPUT, DNS_PROCESSOR, ?, ?, PROCESSOR> connectionBuilder) {
        OutletManager<OUTPUT>.Outlet newOutlet = outletManager().newOutlet();
        Connection<OUTPUT, DNS_PROCESSOR, PROCESSOR> connection =
                connectionBuilder.addOutlet(newOutlet).addUpstreamProcessor(driver()).build();
        newOutlet.attach(connection);
        connection.downstreamProcessor().execute(actor -> actor.setReady(connection));
    }

    protected <PACKET, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> void setReady(Connection<PACKET, PROCESSOR, UPS_PROCESSOR> connection) {
        connection.inlet().attach(connection);
        // TODO: If inlet wants to pull, trigger pulling
    }

    interface Pullable<T> {
        Poller<T> pull();  // Should return a Poller since if there is no answer now there may be in the future
    }

    interface SyncPullable<T> {
        Optional<T> pull();  // Getting back an empty indicates that there will never be an answer
    }

    interface AsyncPullable<T> {
        void pull(Receiver<T> receiver);
    }

    interface Receiver<T> {
        void receiveOrRetry(AsyncPullable<T> upstream, T packet);
    }

    // TODO: Note that the identifier for an upstream controller (e.g. resolvable) is different to for an upstream processor (resolvable plus bounds). So inletmanagers are managed based on the former.

    static abstract class InletManager<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> implements Pullable<INPUT> {

        public abstract Inlet newInlet();  // TODO: Should be called by a handler in the controller

        public abstract Set<Inlet> inlets();

        public static class Single<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INPUT, UPS_PROCESSOR> {

            @Override
            public Poller<INPUT> pull() {
                return Pollers.empty(); // TODO
            }

            @Override
            public InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet() {
                // TODO: Allow one inlet to be established either via this method or via constructor, and after that throw an exception
                throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public Set<Inlet> inlets() {
                return null;  // TODO
            }

        }

        static class DynamicMulti<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INPUT, UPS_PROCESSOR> {

            Set<Inlet> inlets;  // TODO: Does this need to be a map?

            DynamicMulti() {
                this.inlets = new HashSet<>();
            }

            @Override
            public InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet() {
                InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet = new Inlet();
                inlets.add(newInlet);
                return newInlet;
            }

            @Override
            public Set<Inlet> inlets() {
                return inlets;
            }

            @Override
            public Poller<INPUT> pull() {
                // TODO: Get the next inlet and pull from it
                // TODO: How will this work without blocking to see if an answer is returned? Likely we will always end
                //  up requesting a pull from more than one downstream even if the first would have sufficed. This is
                //  because we can't guarantee that any inlet will ever fail (it could be a cycle)

                // TODO: Implement polling behaviour
                // for (Inlet inlet : inlets.values()) {
                //     if (inlet.hasPacketReady()) return Optional.of(inlet.nextPacket());
                // }
                // for (Inlet inlet : inlets.values()) inlet.pull();
                return Pollers.empty();
            }
        }

        class Inlet implements AsyncPullable<INPUT>, Receiver<INPUT> {

            private Processor.Connection<INPUT, ?, UPS_PROCESSOR> connection;
            private boolean isPulling;

            protected Inlet() {
                this.isPulling = false;
            }

            protected boolean hasPacketReady() {
                return true;  // TODO
            }

            protected INPUT nextPacket() {
                return null;  // TODO: return any buffered packets
            }

            @Override
            public void pull(Receiver<INPUT> receiver) {
                if (!isPulling) {
                    connection.upstreamProcessor().execute(actor -> actor.pullPacket(connection));
                    isPulling = true;
                }
            }

            public void attach(Connection<INPUT, ?, UPS_PROCESSOR> connection) {  // TODO: Connection type PROCESSOR should be PROCESSOR
                this.connection = connection;
            }

            @Override
            public void receiveOrRetry(AsyncPullable<INPUT> upstream, INPUT packet) {

            }
        }

    }

    public void pullPacket(Connection<OUTPUT, ?, ?> connection) {
        outletManager().pull(connection);
    }

    public <PACKET> void receivePacket(Connection<PACKET, ?, ?> connection, PACKET packet) {
        connection.inlet().receiveOrRetry(connection.outlet(), packet);  // TODO: It's weird that this doesn't require any state to work beacuse the connection already has the knowledge
    }

    public static abstract class OutletManager<OUTPUT> implements AsyncPullable<OUTPUT>, Receiver<OUTPUT> {

        private final Supplier<OUTPUT> onPull;

        OutletManager(Supplier<OUTPUT> onPull) {
            this.onPull = onPull;
        }

        public void pull(Connection<OUTPUT, ?, ?> connection) {
            onPull.get();
        }

        @Override
        public void receiveOrRetry(AsyncPullable<OUTPUT> upstream, OUTPUT packet) {
            outlets().forEach(outlet -> outlet.receiveOrRetry(upstream, packet));
        }

        abstract Set<Outlet> outlets();

        public abstract void feed(ReactiveTransform<?, OUTPUT> op);

        public abstract Outlet newOutlet();

        public static class Single<OUTPUT> extends OutletManager<OUTPUT> {

            private final OutletManager<OUTPUT>.Outlet outlet;

            Single(OutletManager<OUTPUT>.Outlet outlet, Supplier<OUTPUT> onPull) {
                super(onPull);
                this.outlet = outlet;
            }

            @Override
            Set<Outlet> outlets() {
                return set(outlet);
            }

            @Override
            public void feed(ReactiveTransform<?, OUTPUT> op) {
                // TODO
            }

            @Override
            public Outlet newOutlet() {
                throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public void pull(Receiver<OUTPUT> receiver) {
                // TODO
            }
        }

        public static class DynamicMulti<OUTPUT> extends OutletManager<OUTPUT> {

            private final Set<OutletManager<OUTPUT>.Outlet> outlets;

            DynamicMulti(Supplier<OUTPUT> onPull) {
                super(onPull);
                this.outlets = new HashSet<>();
            }

            @Override
            Set<Outlet> outlets() {
                return outlets;
            }

            @Override
            public void feed(ReactiveTransform<?, OUTPUT> op) {
                // TODO
            }

            @Override
            public Outlet newOutlet() {
                OutletManager<OUTPUT>.Outlet newOutlet = new Outlet();
                outlets.add(newOutlet);
                return newOutlet;
            }

            @Override
            public void pull(Receiver<OUTPUT> receiver) {
                // TODO
            }
        }

        class Outlet implements AsyncPullable<OUTPUT>, Receiver<OUTPUT> {

            private Connection<OUTPUT, ?, ?> connection;

            public void attach(Connection<OUTPUT, ?, ?> connection) {  // TODO: The connection UPS_PROCESSOR type should be PROCESSOR
                this.connection = connection;
            }

            @Override
            public void receiveOrRetry(AsyncPullable<OUTPUT> upstream, OUTPUT packet) {
                // TODO: If pulling then send packet on, otherwise buffer it?
                connection.downstreamProcessor().execute(actor -> actor.receivePacket(connection, packet));
            }

            @Override
            public void pull(Receiver<OUTPUT> receiver) {
                // TODO
            }
        }

    }

    static class Source<INPUT> implements AsyncPullable<INPUT> {

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
        public void pull(Receiver<INPUT> receiver) {
            if (iterator == null) iterator = iteratorSupplier.get();
            if (iterator.hasNext()) {
                receiver.receiveOrRetry(this, iterator.next());
            }
        }
    }

    public static abstract class Reactive<INPUT, OUTPUT> implements AsyncPullable<INPUT>, Receiver<INPUT> {

        protected final Set<Receiver<OUTPUT>> downstreams;
        protected final Set<AsyncPullable<INPUT>> upstreams;
        private boolean isPulling;

        Reactive(Set<Receiver<OUTPUT>> downstreams, Set<AsyncPullable<INPUT>> upstreams) {
            this.downstreams = downstreams;
            this.upstreams = upstreams;
            this.isPulling = false;
        }

        @Override
        public void pull(Receiver<INPUT> receiver) {
            if (!isPulling) {
                upstreams.forEach(this::upstreamPull);
                isPulling = true;
            }
        }

        @Override
        public void receiveOrRetry(AsyncPullable<INPUT> upstream, INPUT packet) {
            FunctionalIterator<OUTPUT> transformed = transform(packet);
            if (transformed.hasNext()) {
                transformed.forEachRemaining(t -> downstreams.forEach(downstream -> downstreamReceive(downstream, t)));
                isPulling = false;
            } else if (isPulling) {
                upstreamPull(upstream);  // Automatic retry
            }
        }

        private void downstreamReceive(Receiver<OUTPUT> downstream, OUTPUT p) {
            // TODO: Override for cross-actor receiving
            downstream.receiveOrRetry((AsyncPullable<OUTPUT>) this, p);  // TODO: Remove casting
        }

        protected void upstreamPull(AsyncPullable<INPUT> upstream) {
            // TODO: Override for cross-actor pulling
            upstream.pull(this);
        }

        protected abstract FunctionalIterator<OUTPUT> transform(INPUT packet);

    }

    public static class TransientReactive<T> extends Reactive<T, T> {

        TransientReactive(Set<Receiver<T>> downstreams, Set<AsyncPullable<T>> upstreams) {
            super(downstreams, upstreams);
        }

        @Override
        public void receiveOrRetry(AsyncPullable<T> upstream, T packet) {
            downstreams.forEach(downstream -> downstream.receiveOrRetry(upstream, packet));
        }

        @Override
        protected FunctionalIterator<T> transform(T packet) {
            return Iterators.single(packet);
        }
    }

    public static abstract class ReactiveTransform<INPUT, OUTPUT> extends Reactive<INPUT, OUTPUT> {
        ReactiveTransform(Set<Receiver<OUTPUT>> downstreams, Set<AsyncPullable<INPUT>> upstreams) {
            super(downstreams, upstreams);
        }

        public static <I> ReactiveTransform<I, I> input(Pullable<I> input) {
            return null;  // TODO
        }

        public static <I> ReactiveTransform<I, I> inputs(Collection<Pullable<I>> inputs) {
            return null;  // TODO
        }

        public static <I> ReactiveTransform<I, I> fromIterator(FunctionalIterator<I> input) {
            return null;
        }

        public static <R, T> ReactiveTransform<R, T> sourceJoin(Source<T> source, ReactiveTransform<R, T> operation) {
            return null;  // TODO
        }

        public static <R> ReactiveTransform<?, R> input(FunctionalIterator<R> flatMap) {
            return null;  // TODO
        }

        abstract <NEW_OUTPUT> ReactiveTransform<INPUT, NEW_OUTPUT> flatMap(Function<OUTPUT, ReactiveTransform<OUTPUT, NEW_OUTPUT>> function);

        abstract <NEW_OUTPUT> ReactiveTransform<INPUT, NEW_OUTPUT> map(Function<OUTPUT, NEW_OUTPUT> function);

        abstract void forEach(Consumer<INPUT> function);

        abstract ReactiveTransform<INPUT, OUTPUT> filter(Function<OUTPUT, Boolean> function);

        abstract ReactiveTransform<INPUT, OUTPUT> findFirst();

    }

    public static class Connection<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> {

        private final Driver<PROCESSOR> downstreamProcessor;
        private final Driver<UPS_PROCESSOR> upstreamProcessor;
        private final InletManager<PACKET, UPS_PROCESSOR>.Inlet inlet;
        private final OutletManager<PACKET>.Outlet outlet;

        public Connection(Driver<PROCESSOR> downstreamProcessor, Driver<UPS_PROCESSOR> upstreamProcessor, InletManager<PACKET, UPS_PROCESSOR>.Inlet inlet, OutletManager<PACKET>.Outlet outlet) {
            this.downstreamProcessor = downstreamProcessor;
            this.upstreamProcessor = upstreamProcessor;
            this.inlet = inlet;
            this.outlet = outlet;
        }

        private Driver<UPS_PROCESSOR> upstreamProcessor() {
            return upstreamProcessor;
        }

        Driver<PROCESSOR> downstreamProcessor() {
            return downstreamProcessor;
        }

        OutletManager<?>.Outlet outlet() {
            return outlet;
        }

        InletManager<PACKET, UPS_PROCESSOR>.Inlet inlet() {
            return inlet;
        }

        public static class Builder<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, UPS_CID, UPS_PID, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> {

            private final Driver<PROCESSOR> downstreamProcessor;
            private final UPS_CID upstreamControllerId;
            private Driver<UPS_PROCESSOR> upstreamProcessor;
            private final UPS_PID upstreamProcessorId;
            private final InletManager<PACKET, UPS_PROCESSOR>.Inlet inlet;
            private OutletManager<PACKET>.Outlet outlet;

            protected Builder(Driver<PROCESSOR> downstreamProcessor, UPS_CID upstreamControllerId,
                              UPS_PID upstreamProcessorId, InletManager<PACKET, UPS_PROCESSOR>.Inlet inlet) {
                this.downstreamProcessor = downstreamProcessor;
                this.upstreamControllerId = upstreamControllerId;
                this.upstreamProcessorId = upstreamProcessorId;
                this.inlet = inlet;
            }

            UPS_CID upstreamControllerId() {
                return upstreamControllerId;
            }


            protected Builder<PACKET, PROCESSOR, UPS_CID, UPS_PID, UPS_PROCESSOR> addOutlet(OutletManager<PACKET>.Outlet outlet) {
                this.outlet = outlet;
                return this;
            }

            protected Builder<PACKET, PROCESSOR, UPS_CID, UPS_PID, UPS_PROCESSOR> addUpstreamProcessor(Driver<UPS_PROCESSOR> upstreamProcessor) {
                this.upstreamProcessor = upstreamProcessor;
                return this;
            }

            Connection<PACKET, PROCESSOR, UPS_PROCESSOR> build() {
                assert downstreamProcessor != null;
                assert upstreamProcessor != null;
                assert inlet != null;
                assert outlet != null;
                return new Connection<>(downstreamProcessor, upstreamProcessor, inlet, outlet);
            }

            public UPS_PID upstreamProcessorId() {
                return upstreamProcessorId;
            }
        }
    }
}
