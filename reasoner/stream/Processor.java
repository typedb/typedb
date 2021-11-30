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
import com.vaticle.typedb.core.common.poller.Poller;
import com.vaticle.typedb.core.common.poller.Pollers;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<?, ?, OUTPUT, PROCESSOR, ?>> controller;
    private final OutletManager<OUTPUT> outletManager;

    public Processor(Driver<PROCESSOR> driver,
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

    protected <UPS_CID, UPS_PID> void requestUpstreamProcessor(UPS_CID controllerId, UPS_PID processorId) {  // TODO: naming leaks domain
        // TODO: Can be called when:
        //  1. initialising a fixed set of upstream processors (would we like to do this a non async way instead?)
        //  2. an answer is found in a conjunction and is passed to the sibling
        //  3. an answer from a condition is passed up and needs to be materialised only when granted a lease from a lease processor
        // Starts a series of messages that will add a new inlet stream to the processor from a processor of the given id
        controller.execute(actor -> actor.getUpstreamTransceiver(controllerId, null).receiveUpstreamProcessorRequest(controllerId, processorId, driver()));
    }

    protected <UPS_CID, UPS_PID, UPS_OUTPUT, UPS_PROCESSOR extends Processor<UPS_OUTPUT, UPS_PROCESSOR>> void receiveUpstreamProcessor(
            UPS_CID controllerId, Driver<UPS_PROCESSOR> processor
    ) {
        Connection.Builder<UPS_OUTPUT, PROCESSOR, UPS_PROCESSOR> connectionBuilder = new Connection.Builder<>(driver(), processor);
        InletManager<UPS_PID, UPS_OUTPUT, UPS_PROCESSOR> inletManager = getInletManager(controllerId);
        connectionBuilder.addInlet(inletManager.newInlet());
        processor.execute(actor -> buildConnection(connectionBuilder));
    }

    protected void buildConnection(Connection.Builder<OUTPUT, ?, PROCESSOR> connectionBuilder) {
        OutletManager<OUTPUT>.Outlet newOutlet = outletManager().newOutlet();
        Connection<?, PROCESSOR> connection = connectionBuilder.addOutlet(newOutlet).build();
        newOutlet.attach(connection);
        connection.downstreamProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected void finaliseConnection(Connection<PROCESSOR, UPS_PROCESSOR> connection) {
        connection.inlet().attach(connection);
    }

    // TODO: InletManagers are identified by upstream controller ids. These types are unknown so should be handled by child class, which will require casting
    public abstract <INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR extends Processor<INPUT, UPSTREAM_PROCESSOR>> InletManager<INLET_ID, INPUT, UPSTREAM_PROCESSOR> getInletManager(INLET_MANAGER_ID controllerId);

    interface Pullable<T> {
        Poller<T> pull();  // Should return a Poller since if there is no answer now there may be in the future
    }

    interface SyncPullable<T> {
        Optional<T> pull();  // Getting back an empty indicates that there will never be an answer
    }

    interface AsyncPullable {
        void pull();
    }

    // TODO: Note that the identifier for an upstream controller (e.g. resolvable) is different to for an upstream processor (resolvable plus bounds). So inletmanagers are managed based on the former.

    static abstract class InletManager<INLET_ID, INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> implements Pullable<INPUT> {

        public abstract Inlet newInlet();  // TODO: Should be called by a handler in the controller

        public static class Single<INLET_ID, INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INLET_ID, INPUT, UPS_PROCESSOR> {

            @Override
            public Poller<INPUT> pull() {
                return Pollers.empty(); // TODO
            }

            @Override
            public InletManager<INLET_ID, INPUT, UPS_PROCESSOR>.Inlet newInlet() {
                // TODO: Allow one inlet to be established either via this method or via constructor, and after that throw an exception
                throw TypeDBException.of(ILLEGAL_STATE);
            }

        }

        static class DynamicMulti<INLET_ID, INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>> extends InletManager<INLET_ID, INPUT, UPS_PROCESSOR> {

            Set<Inlet> inlets;  // TODO: Does this need to be a map?

            DynamicMulti() {
                this.inlets = new HashSet<>();
            }

            @Override
            public InletManager<INLET_ID, INPUT, UPS_PROCESSOR>.Inlet newInlet() {
                InletManager<INLET_ID, INPUT, UPS_PROCESSOR>.Inlet newInlet = new Inlet();
                inlets.add(newInlet);
                return newInlet;
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

        class Inlet implements AsyncPullable {

            private Processor.Connection<?, UPS_PROCESSOR> connection;
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
            public void pull() {
                if (!isPulling) {
                    connection.upstreamProcessor().execute(actor -> actor.pullPacket(connection));
                    isPulling = true;
                }
            }

            public void attach(Connection<?, UPS_PROCESSOR> connection) {  // TODO: Connection type PROCESSOR should be PROCESSOR
                this.connection = connection;
            }
        }

    }

    public void pullPacket(Connection<?, ?> connection) {
        outletManager().pull(connection);
    }

    public <PACKET> void receivePacket(Connection<?, ?> connection, PACKET packet) {
        // TODO
    }

    public static abstract class OutletManager<OUTPUT> implements AsyncPullable {

        private final Supplier<OUTPUT> onPull;

        OutletManager(Supplier<OUTPUT> onPull) {
            this.onPull = onPull;
        }

        public void pull(Connection<?, ?> connection) {
            onPull.get();
        }

        public void receivePacket(OUTPUT packet) {
            Connection<?, ?> connection = null;
            connection.downstreamProcessor().execute(actor -> actor.receivePacket(connection, packet));
        }

        public abstract void feed(Operation<?, OUTPUT> op);

        public abstract Outlet newOutlet();

        public static class Single<OUTPUT> extends OutletManager<OUTPUT> {

            Single(Supplier<OUTPUT> onPull) {
                super(onPull);
            }

            @Override
            public void feed(Operation<?, OUTPUT> op) {
                // TODO
            }

            @Override
            public Outlet newOutlet() {
                return new Outlet();
            }

            @Override
            public void pull() {
                // TODO
            }
        }

        public static class DynamicMulti<OUTPUT> extends OutletManager<OUTPUT> {

            DynamicMulti(Supplier<OUTPUT> onPull) {
                super(onPull);
            }

            @Override
            public void feed(Operation<?, OUTPUT> op) {
                // TODO
            }

            @Override
            public Outlet newOutlet() {
                return new Outlet();
            }

            @Override
            public void pull() {
                // TODO
            }
        }

        class Outlet {

            public void attach(Connection<?, ?> connection) {  // TODO: The connection UPS_PROCESSOR type should be PROCESSOR
                // TODO
            }
        }

    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<ConceptMap>> traversal) {
            return null;  // TODO
        }

        public Operation<INPUT, INPUT> asOperation() {
            return null; // TODO
        }
    }

    public static abstract class Operation<INPUT, OUTPUT> {
        public static <I> Operation<I, I> input(Pullable<I> input) {
            return null;  // TODO
        }

        public static <I> Operation<I, I> inputs(Collection<Pullable<I>> inputs) {
            return null;  // TODO
        }

        public static <R, T> Operation<R, T> sourceJoin(Source<T> source, Operation<R, T> operation) {
            return null;  // TODO
        }

        public static Operation<?, ConclusionController.ConclusionAns> input(FunctionalIterator<ConceptMap> flatMap) {
            return null;  // TODO
        }

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> flatMap(Function<OUTPUT, Operation<OUTPUT, NEW_OUTPUT>> function);

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> map(Function<OUTPUT, NEW_OUTPUT> function);

        abstract void forEach(Consumer<INPUT> function);

        abstract Operation<INPUT, OUTPUT> filter(Function<OUTPUT, Boolean> function);

        abstract Operation<INPUT, OUTPUT> findFirst();

    }

    public static class Connection<PROCESSOR extends Processor<?, PROCESSOR>, UPS_PROCESSOR extends Processor<?, UPS_PROCESSOR>> {

        private final Driver<PROCESSOR> downstreamProcessor;
        private final Driver<UPS_PROCESSOR> upstreamProcessor;
        private final InletManager<?, ?, UPS_PROCESSOR>.Inlet inlet;
        private final OutletManager<?>.Outlet outlet;

        public Connection(Driver<PROCESSOR> downstreamProcessor, Driver<UPS_PROCESSOR> upstreamProcessor, InletManager<?, ?, UPS_PROCESSOR>.Inlet inlet, OutletManager<?>.Outlet outlet) {
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

        InletManager<?, ?, UPS_PROCESSOR>.Inlet inlet() {
            return inlet;
        }

        public static class Builder<PACKET, PROCESSOR extends Processor<?, PROCESSOR>, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> {

            private final Driver<PROCESSOR> downstreamProcessor;
            private final Driver<UPS_PROCESSOR> upstreamProcessor;
            private InletManager<?, ?, UPS_PROCESSOR>.Inlet inlet;
            private OutletManager<PACKET>.Outlet outlet;

            protected Builder(Driver<PROCESSOR> downstreamProcessor, Driver<UPS_PROCESSOR> upstreamProcessor) {
                this.downstreamProcessor = downstreamProcessor;
                this.upstreamProcessor = upstreamProcessor;
            }

            protected Builder<PACKET, PROCESSOR, UPS_PROCESSOR> addInlet(InletManager<?, PACKET, UPS_PROCESSOR>.Inlet inlet) {
                this.inlet = inlet;
                return this;
            }

            protected Builder<PACKET, PROCESSOR, UPS_PROCESSOR> addOutlet(OutletManager<PACKET>.Outlet outlet) {
                this.outlet = outlet;
                return this;
            }

            Connection<PROCESSOR, UPS_PROCESSOR> build() {
                return new Connection<>(downstreamProcessor, upstreamProcessor, inlet, outlet);
            }
        }
    }
}
