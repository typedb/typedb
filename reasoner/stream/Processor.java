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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.function.Function;

public class Processor<INPUT, OUTPUT, INLET extends Processor.Inlet<INPUT>, OUTLET extends Processor.Outlet<OUTPUT>>
        extends Actor<Processor<INPUT, OUTPUT, INLET, OUTLET>> {

    private final Operation<INPUT, OUTPUT> operation;
    private final INLET inlet;
    private final OUTLET outlet;

    public Processor(Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> driver, String name,
                     Operation<INPUT, OUTPUT> operation, INLET inlet, OUTLET outlet) {
        super(driver, name);
        this.operation = operation;
        this.inlet = inlet;
        this.outlet = outlet;
        this.operation.connect(this.inlet);
        this.outlet.connect(this.operation);
    }

    public INLET inlet() {
        return inlet;
    }

    public OUTLET outlet() {
        return outlet;
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
            public void add(Driver<? extends Processor<OUTPUT, ?, ?, ?>> newOutletPipe) {
                // TODO: Store the pipe
            }
        }
    }

    public static class Inlet<INPUT> {
        public static class Single<INPUT> extends Inlet<INPUT> {}
        public static class DynamicMulti<INPUT> extends Inlet<INPUT> {
            public void add(Driver<? extends Processor<?, INPUT, ?, ?>> newInletPipe) {
                // TODO: Store the pipe
            }
        }
    }

    public static abstract class Operation<INPUT, OUTPUT> {
        public static <T> Operation<T, T> input() {
            return null;  // TODO
        }

        public static <T> Operation<T, T> orderedJoin(Operation<?, T> operation1, Operation<?, T> operation2) {
            return null;  // TODO
        }

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> flatMapOrRetry(Function<OUTPUT, NEW_OUTPUT> function);

        abstract <NEW_OUTPUT> Operation<INPUT, NEW_OUTPUT> map(Function<OUTPUT, NEW_OUTPUT> function);

        abstract Operation<INPUT, OUTPUT> filter(Function<OUTPUT, Boolean> function);

        abstract Operation<INPUT, OUTPUT> findFirst();

        abstract Operation<INPUT, OUTPUT> buffer(Buffer<OUTPUT> buffer);

        abstract void connect(Inlet<INPUT> inlet);
    }

    public static class Buffer<T> {}
}
