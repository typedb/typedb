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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Processor<INPUT, OUTPUT> extends Actor<Processor<INPUT, OUTPUT>> {

    private final Operation<INPUT, OUTPUT> operation;
    private final Inlet<INPUT> inlet;
    private final Outlet<OUTPUT> outlet;

    public Processor(Driver<Processor<INPUT, OUTPUT>> driver, String name, Operation<INPUT, OUTPUT> operation,
                     boolean dynamicInlets, boolean dynamicOutlets) {
        super(driver, name);
        this.operation = operation;
        this.inlet = dynamicInlets ? new Inlet.DynamicMulti<>() : new Inlet.Single<>();
        this.outlet = dynamicOutlets ? new Outlet.DynamicMulti<>() : new Outlet.DynamicMulti<>();
        this.operation.connect(this.inlet);
        this.outlet.connect(this.operation);
    }

    public Inlet<INPUT> inlet() {
        return inlet;
    }

    public Outlet<OUTPUT> outlet() {
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
            @Override
            public <DOWNSTREAM_OUTPUT> void add(Driver<Processor<OUTPUT, DOWNSTREAM_OUTPUT>> newOutlet) {
                // TODO: Store the new outlet
            }
        }
    }

    public static class Inlet<INPUT> {
        public static class Single<INPUT> extends Inlet<INPUT> {}
        public static class DynamicMulti<INPUT> extends Inlet<INPUT> {
            @Override
            public <UPSTREAM_INPUT> void add(Driver<Processor<UPSTREAM_INPUT, INPUT>> newInlet) {
                // TODO: Store the new inlet
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
