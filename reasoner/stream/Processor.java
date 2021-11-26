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
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.stream.Controller.Source;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Processor<CONTROLLER_ID, PROCESSOR_ID, OUTPUT, PROCESSOR extends Processor<CONTROLLER_ID, PROCESSOR_ID, OUTPUT, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<CONTROLLER_ID, PROCESSOR_ID, PROCESSOR, OUTPUT, ?>> controller;
    private final OutletManager outletManager;

    public Processor(Driver<PROCESSOR> driver,
                     Driver<? extends Controller<CONTROLLER_ID, PROCESSOR_ID, PROCESSOR, OUTPUT, ?>> controller,
                     String name, OutletManager outletManager) {
        super(driver, name);
        this.controller = controller;
        this.outletManager = outletManager;
    }

    public OutletManager outletManager() {
        return outletManager;
    }

    @Override
    protected void exception(Throwable e) {}

    protected <INLET_ID, UPSTREAM_ID> void requestInletStream(INLET_ID inletId, UPSTREAM_ID processor_id) {
        // TODO: Can be called when:
        //  1. initialising a fixed set of upstream processors (would we like to do this a non async way instead?)
        //  2. an answer is found in a conjunction and is passed to the sibling
        //  3. an answer from a condition is passed up and needs to be materialised only when granted a lease from a lease processor
        // Starts a series of messages that will add a new inlet stream to the processor from a processor of the given id
        controller.execute(actor -> actor.receiveUpstreamProcessorRequest(inletId, processor_id, driver()));
    }

    // TODO: InletManagers are identified by upstream controller ids. These types are unknown so should be handled by child class, which will require casting
    public abstract <INLET_ID, INPUT, UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR extends Processor<UPSTREAM_CONTROLLER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR>> InletManager<INLET_ID, INPUT, UPSTREAM_PROCESSOR> getInletManager(UPSTREAM_CONTROLLER_ID controllerId);

    interface Pullable<T> {
        Optional<T> pull();  // TODO: Never returns anything for async implementations. Is it a smell that this could be sync or async or actually a good abstraction?

    }

    interface AsyncPullable {
        void pull();
    }

    // TODO: Note that the identifier for an upstream controller (e.g. resolvable) is different to for an upstream processor (resolvable plus bounds). So inletmanagers are managed based on the former.

    public abstract class InletManager<INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR extends Processor<?, ?, INPUT, UPSTREAM_PROCESSOR>> implements Pullable<INPUT> {

        public abstract void newInlet(INLET_ID id, Driver<UPSTREAM_PROCESSOR> newInlet);  // TODO: Should be called by a handler in the controller

        public class Single extends InletManager<INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR> {

            @Override
            public Optional<INPUT> pull() {
                return Optional.empty(); // TODO
            }

            @Override
            public void newInlet(INLET_ID id, Driver<UPSTREAM_PROCESSOR> newInlet) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }

        }

        public class DynamicMulti extends InletManager<INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR> {

            LinkedHashMap<INLET_ID, Inlet> inlets;  // TODO: Does this need to be a map?

            DynamicMulti() {
                this.inlets = new LinkedHashMap<>();
            }

            @Override
            public void newInlet(INLET_ID id, Driver<UPSTREAM_PROCESSOR> upstreamProcessor) {
                Connection connection = new Connection(upstreamProcessor, driver());
                upstreamProcessor.execute(actor -> actor.outletManager().makeConnection(connection));  // TODO: Fix warning
                inlets.put(id, new Inlet(connection));
            }

            @Override
            public Optional<INPUT> pull() {
                // TODO: Get the next inlet and pull from it
                // TODO: How will this work without blocking to see if an answer is returned? Likely we will always end
                //  up requesting a pull from more than one downstream even if the first would have sufficed. This is
                //  because we can't guarantee that any inlet will ever fail (it could be a cycle)

                for (Inlet inlet : inlets.values()) {
                    if (inlet.hasPacketReady()) return Optional.of(inlet.nextPacket());
                }
                for (Inlet inlet : inlets.values()) inlet.pull();
                return Optional.empty();
            }
        }

        public class Inlet implements AsyncPullable {

            private final Connection connection;
            private boolean isPulling;

            protected Inlet(Connection connection) {
                this.connection = connection;
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
                    connection.upstreamProcessor().execute(actor -> actor.outletManager().pull());
                    isPulling = true;
                }
            }

        }

        public class Connection {

            private final Driver<PROCESSOR> downstreamProcessor;
            private final Driver<UPSTREAM_PROCESSOR> upstreamProcessor;

            protected Connection(Driver<UPSTREAM_PROCESSOR> upstreamProcessor, Driver<PROCESSOR> downstreamProcessor) {
                this.downstreamProcessor = downstreamProcessor;
                this.upstreamProcessor = upstreamProcessor;
            }

            private Driver<UPSTREAM_PROCESSOR> upstreamProcessor() {
                return upstreamProcessor;
            }

            Driver<PROCESSOR> downstreamProcessor() {
                return downstreamProcessor;
            }

        }

    }

    public abstract class OutletManager<INLET_ID, INPUT> implements AsyncPullable {

        public abstract void makeConnection(InletManager.Connection newOutlet);  // TODO: Fix override error

        public void connect(Operation<?, OUTPUT> operation) {
            // TODO: set the operation to pull from when answers required
        }

        public class Single extends OutletManager {

            @Override
            public void makeConnection(InletManager.Connection newOutlet) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public void pull() {
                // TODO
            }
        }

        public class DynamicMulti extends OutletManager {

            @Override
            public void makeConnection(InletManager.Connection newOutlet) {
                // TODO: Store the new outlet
            }

            @Override
            public void pull() {
                // TODO
            }
        }

    }

    public static abstract class Operation<INPUT, OUTPUT> {
        public static <T> Operation<?, T> input() {
            return null;  // TODO
        }

        public static <R, T> Operation<R, T> sourceJoin(Source<T> source, Operation<R, T> operation) {
            return null;  // TODO
        }

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> flatMap(Function<OUTPUT, NEW_OUTPUT> function);

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> map(Function<OUTPUT, NEW_OUTPUT> function);

        abstract void forEach(Consumer<INPUT> function);

        abstract Operation<INPUT, OUTPUT> filter(Function<OUTPUT, Boolean> function);

        abstract Operation<INPUT, OUTPUT> findFirst();

        abstract Operation<INPUT, OUTPUT> buffer(Buffer<OUTPUT> buffer);

    }

    public static class Buffer<T> {}
}
