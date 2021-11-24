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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.stream.Controller.Source;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Processor<UPSTREAM_ID, INPUT, OUTPUT> extends Actor<Processor<UPSTREAM_ID, INPUT, OUTPUT>> {

    private final Driver<? extends Controller<?, UPSTREAM_ID, INPUT, OUTPUT, ?>> controller;
    private final Operation<INPUT, OUTPUT> operation;
    private final InletManager<INPUT> inletManager;
    private final OutletManager<OUTPUT> outletManager;

    public Processor(Driver<Processor<UPSTREAM_ID, INPUT, OUTPUT>> driver,
                     Driver<? extends Controller<?, UPSTREAM_ID, INPUT, OUTPUT, ?>> controller,
                     String name, Operation<INPUT, OUTPUT> operation,
                     InletManager<INPUT> inletManager, OutletManager<OUTPUT> outletManager) {
        super(driver, name);
        this.controller = controller;
        this.operation = operation;
        this.inletManager = inletManager;
        this.outletManager = outletManager;
        this.operation.connect(this.inletManager);
        this.outletManager.connect(this.operation);
    }

    public InletManager<INPUT> inletManager() {
        return inletManager;
    }

    public OutletManager<OUTPUT> outletManager() {
        return outletManager;
    }

    public ConceptMap bounds() {
        return null;
    }  // TODO: This shouldn't know about ConceptMaps
    //    public State state() {}  // TODO: Consider this instead

    @Override
    protected void exception(Throwable e) {

    }

    public static class Outlet<OUTPUT> {
        public void connect(Operation<?, OUTPUT> operation) {
            // TODO: set the operation to pull from when answers required
        }

        public static class Single<OUTPUT> extends Outlet<OUTPUT> {}
        public static class DynamicMulti<OUTPUT> extends Outlet<OUTPUT> {
            @Override
            public <UPSTREAM_IDENTIFIER, DOWNSTREAM_OUTPUT> void add(Driver<Processor<UPSTREAM_IDENTIFIER, OUTPUT,
                    DOWNSTREAM_OUTPUT>> newOutlet) {
                // TODO: Store the new outlet
            }
        }

    }

    interface Pullable<T> {
        Optional<T> pull();

    }

    public static class Inlet<INPUT> implements Pullable<INPUT> {

        @Override
        public Optional<INPUT> pull() {
            return Optional.empty();
        }

    }

    public static abstract class InletManager<INPUT> implements Pullable<INPUT> {

        public abstract <UPSTREAM_IDENTIFIER, UPSTREAM_INPUT> void add(Driver<Processor<UPSTREAM_IDENTIFIER,
                UPSTREAM_INPUT, INPUT>> newInlet);

        public static class Single<INPUT> extends InletManager<INPUT> {
            @Override
            public Optional<INPUT> pull() {
                return Optional.empty();  // TODO
            }

            @Override
            public <UPSTREAM_IDENTIFIER, UPSTREAM_INPUT> void add(Driver<Processor<UPSTREAM_IDENTIFIER,
                    UPSTREAM_INPUT, INPUT>> newInlet) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        public static class DynamicMulti<INPUT, PROCESSOR_REQ> extends InletManager<INPUT> {

            Set<Inlet<INPUT>> inlets;

            DynamicMulti(FunctionalIterator<PROCESSOR_REQ> processorRequests) {
                this.inlets = new HashSet<>();
            }

            @Override
            public <UPSTREAM_IDENTIFIER, UPSTREAM_INPUT> void add(Driver<Processor<UPSTREAM_IDENTIFIER,
                    UPSTREAM_INPUT, INPUT>> newInlet) {
                // TODO: Store the new inlet
            }

            @Override
            public Optional<INPUT> pull() {
                // TODO: Get the next inlet and pull from it
                // TODO: How will this work without blocking to see if an answer is returned? Likely we will always end
                //  up requesting a pull from more than one downstream even if the first would have sufficed. This is
                //  because we can't guarantee that any inlet will ever fail (it could be a cycle)
                return Optional.empty();  // TODO
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

        abstract void connect(InletManager<INPUT> inlet);
    }

    public static class Buffer<T> {}
}
