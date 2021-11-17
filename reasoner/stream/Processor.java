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

public class Processor<INLET extends Processor.Inlet, OUTLET extends Processor.Outlet> extends Actor<Processor<INLET, OUTLET>> {

    private final Pipe<INPUT, T> pipe;
    private final INLET inlet;
    private final OUTLET outlet;

    public Processor(Driver<Processor<INLET, OUTLET>> driver, String name, Pipe<INPUT, T> pipe, INLET inlet, OUTLET outlet) {
        super(driver, name);
        this.pipe = pipe;
        this.inlet = inlet;
        this.outlet = outlet;
        this.pipe.connect(this.inlet);
        this.outlet.connect(this.pipe);
    }

    public INLET inlet() {}

    public OUTLET outlet() {}

    public ConceptMap bounds() {}  // TODO: This shouldn't know about ConceptMaps

    @Override
    protected void exception(Throwable e) {

    }

    public static class Outlet {
        public void connect(Pipe<INPUT, T> pipe) {
            // TODO: set the pipe to pull from when answers required
        }

        public static class Single extends Outlet {}
        public static class DynamicMulti extends Outlet {
            public void add(Driver<? extends Processor<?, ?>> newOutlet) {}
        }
    }
    public static class Inlet {
        public static class Single extends Inlet {}
        public static class DynamicMulti extends Inlet {
            public void add() {}
        }
    }

    public static class Pipe<INPUT, OUTPUT> {
        public static <T> Pipe<T, T> input() {
        }

        public static <T> Pipe<T, T> orderedJoin(Pipe<?, T> pipe1, Pipe<?, T> pipe2) {
        }

        public <NEW_OUTPUT> Pipe<INPUT, NEW_OUTPUT> flatMapOrRetry(Function<OUTPUT, NEW_OUTPUT> function) {

        }

        public <NEW_OUTPUT> Pipe<INPUT, NEW_OUTPUT> map(Function<OUTPUT, NEW_OUTPUT> function) {

        }

        public Pipe<INPUT, OUTPUT> filter(Function<OUTPUT, Boolean> function) {

        }

        public Pipe<INPUT, OUTPUT> findFirst() {

        }

        public Pipe<INPUT, OUTPUT> buffer(Buffer buffer) {

        }

        public void connect(Inlet inlet) {

        }
    }

    private static class Buffer {}
}
