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
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.Collection;
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
        void pull(Receiver<T> receiver);
    }

    interface Receiver<T> {
        void receiveOrRetry(Pullable<T> upstream, T packet);
    }

    // TODO: Note that the identifier for an upstream controller (e.g. resolvable) is different to for an upstream processor (resolvable plus bounds). So inletmanagers are managed based on the former.

    static abstract class InletManager<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends TransientReactive<INPUT> {

        InletManager(Receiver<INPUT> downstream, Set<Pullable<INPUT>> upstreams) {
            super(downstream, upstreams);
        }

        public abstract Inlet newInlet();  // TODO: Should be called by a handler in the controller

        public Set<Pullable<INPUT>> inlets() {  // TODO: Can we avoid needing this?
            return upstreams();
        }

        public static class Single<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INPUT, UPS_PROCESSOR> {

            Single(Receiver<INPUT> downstream, Pullable<INPUT> upstream) {
                super(downstream, set(upstream));
            }

            @Override
            public InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet() {
                // TODO: Allow one inlet to be established either via this method or via constructor, and after that throw an exception
                throw TypeDBException.of(ILLEGAL_STATE);
            }

        }

        static class DynamicMulti<INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INPUT, UPS_PROCESSOR> {

            DynamicMulti(Receiver<INPUT> downstream, Set<Pullable<INPUT>> upstreams) {
                super(downstream, upstreams);
            }

            @Override
            public InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet() {  // TODO: Dynamically adding upstreams should be handled by the reactive component
                InletManager<INPUT, UPS_PROCESSOR>.Inlet newInlet = new Inlet(null, this);
                addUpstream(newInlet);
                return newInlet;
            }

        }

        class Inlet extends TransientReactive<INPUT> {  // TODO: Alter this to also do an arbitrary transformation at the inlet (could be kept simple as a transform from of <T, T> to only support mapping not unifiers), passed in in the constructor

            private Processor.Connection<INPUT, ?, UPS_PROCESSOR> connection;

            Inlet(Receiver<INPUT> downstream, Pullable<INPUT> upstream) {
                super(downstream, upstream);
            }

            @Override
            protected void upstreamPull(Pullable<INPUT> upstream) {
                // TODO: choose one of these:
                connection.upstreamProcessor().execute(actor -> upstream.pull(this));
                connection.upstreamProcessor().execute(actor -> connection.outlet().pull(connection.inlet()));
                connection.upstreamProcessor().execute(actor -> actor.pullPacket(connection));
            }

            public void attach(Connection<INPUT, ?, UPS_PROCESSOR> connection) {  // TODO: Connection type PROCESSOR should be PROCESSOR
                this.connection = connection;
            }

        }

    }

    public void pullPacket(Connection<OUTPUT, ?, ?> connection) {
        connection.outlet().pull(connection.inlet());
    }

    public <PACKET> void receivePacket(Connection<PACKET, ?, ?> connection, PACKET packet) {
        connection.inlet().receiveOrRetry(connection.outlet(), packet);  // TODO: It's weird that this doesn't require any state to work because the connection already has the knowledge
    }

    public static abstract class OutletManager<OUTPUT> extends TransientReactive<OUTPUT> {

        OutletManager(Set<Receiver<OUTPUT>> downstreams, Pullable<OUTPUT> upstream) {
            super(downstreams, upstream);
        }

        public abstract Outlet newOutlet();

        public static class Single<OUTPUT> extends OutletManager<OUTPUT> {

            Single(Receiver<OUTPUT> downstream, Pullable<OUTPUT> upstream) {
                super(set(downstream), upstream);
            }

            @Override
            public Outlet newOutlet() {
                throw TypeDBException.of(ILLEGAL_STATE);
            }

        }

        public static class DynamicMulti<OUTPUT> extends OutletManager<OUTPUT> {

            DynamicMulti(Set<Receiver<OUTPUT>> downstreams, Pullable<OUTPUT> upstream) {
                super(downstreams, upstream);
            }

            @Override
            public Outlet newOutlet() {
                // TODO: Handle dynamically adding outlets in the reactive components
                OutletManager<OUTPUT>.Outlet newOutlet = new Outlet(connection, this);
                addDownstream(newOutlet);
                return newOutlet;
            }

        }

        class Outlet extends TransientReactive<OUTPUT> {

            private Connection<OUTPUT, ?, ?> connection;

            Outlet(Receiver<OUTPUT> downstream, Pullable<OUTPUT> upstream) {
                super(downstream, upstream);
            }

            public void attach(Connection<OUTPUT, ?, ?> connection) {  // TODO: The connection UPS_PROCESSOR type should be PROCESSOR
                assert set(connection.inlet()).equals(upstreams());
                this.connection = connection;
            }

            @Override
            protected void downstreamReceive(Receiver<OUTPUT> downstream, OUTPUT packet) {
                // TODO: Choose one of these to use
                connection.downstreamProcessor().execute(actor -> actor.receivePacket(connection, packet));
                connection.downstreamProcessor().execute(actor -> downstream.receiveOrRetry(this, packet));
            }
        }

    }

    static class Source<INPUT> implements Pullable<INPUT> {

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

    public static abstract class Reactive<INPUT, OUTPUT> implements Pullable<INPUT>, Receiver<INPUT> {

        private final Set<Receiver<OUTPUT>> downstreams;
        private final Set<Pullable<INPUT>> upstreams;
        private boolean isPulling;

        Reactive(Set<Receiver<OUTPUT>> downstreams, Set<Pullable<INPUT>> upstreams) {
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

        Set<Receiver<OUTPUT>> downstreams() {
            return downstreams;
        }

        Set<Pullable<INPUT>> upstreams() {
            return upstreams;
        }

        protected void addDownstream(Receiver<OUTPUT> downstream) {
            downstreams.add(downstream);
            // TODO: To dynamically add downstreams we need to have buffered all prior packets and send them here
        }

        protected void addUpstream(Pullable<INPUT> upstream) {
            upstreams.add(upstream);
            if (isPulling) upstream.pull(this);
        }

        @Override
        public void receiveOrRetry(Pullable<INPUT> upstream, INPUT packet) {
            FunctionalIterator<OUTPUT> transformed = transform(packet);
            if (transformed.hasNext()) {
                transformed.forEachRemaining(t -> downstreams.forEach(downstream -> downstreamReceive(downstream, t)));
                isPulling = false;
            } else if (isPulling) {
                upstreamPull(upstream);  // Automatic retry
            }
        }

        protected void downstreamReceive(Receiver<OUTPUT> downstream, OUTPUT p) {
            // TODO: Override for cross-actor receiving
            downstream.receiveOrRetry((Pullable<OUTPUT>) this, p);  // TODO: Remove casting
        }

        protected void upstreamPull(Pullable<INPUT> upstream) {
            // TODO: Override for cross-actor pulling
            upstream.pull(this);
        }

        protected abstract FunctionalIterator<OUTPUT> transform(INPUT packet);

    }

    public static class TransientReactive<T> extends Reactive<T, T> {

        TransientReactive(Set<Receiver<T>> downstreams, Set<Pullable<T>> upstreams) {
            super(downstreams, upstreams);
        }

        TransientReactive(Receiver<T> downstream, Set<Pullable<T>> upstreams) {
            super(set(downstream), upstreams);
        }

        TransientReactive(Set<Receiver<T>> downstreams, Pullable<T> upstream) {
            super(downstreams, set(upstream));
        }

        TransientReactive(Receiver<T> downstream, Pullable<T> upstream) {
            super(set(downstream), set(upstream));
        }

        @Override
        protected FunctionalIterator<T> transform(T packet) {
            return Iterators.single(packet);
        }
    }

    public static abstract class ReactiveTransform<INPUT, OUTPUT> extends Reactive<INPUT, OUTPUT> {
        ReactiveTransform(Set<Receiver<OUTPUT>> downstreams, Set<Pullable<INPUT>> upstreams) {
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

        OutletManager<PACKET>.Outlet outlet() {
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
