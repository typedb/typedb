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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.Objects;

public class ReactiveIdentifier implements Reactive.Identifier {
    private final Actor.Driver<? extends Processor<?, ?, ?, ?>> processor;
    private final Class<? extends Reactive> reactiveClass;
    private final int scopedId;

    public ReactiveIdentifier(Actor.Driver<? extends Processor<?, ?, ?, ?>> processor, Class<? extends Reactive> reactiveClass, int scopedId) {
        this.processor = processor;
        this.reactiveClass = reactiveClass;
        this.scopedId = scopedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReactiveIdentifier that = (ReactiveIdentifier) o;
        return scopedId == that.scopedId &&
                processor.equals(that.processor) &&
                reactiveClass.equals(that.reactiveClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processor, reactiveClass, scopedId);
    }

    @Override
    public String toString() {
        return processor.debugName().get() + ":" + reactiveClass +":" + scopedId;
    }

    @Override
    public Actor.Driver<? extends Processor<?, ?, ?, ?>> processor() {
        return processor;
    }

    @Override
    public Identifier identifier() {
        return this;
    }

    public static class Output<PACKET> extends ReactiveIdentifier implements Reactive.Identifier.Output<PACKET> {

        private final Actor.Driver<? extends Processor<?, PACKET, ?, ?>> processor;  //TODO: Duplicates field from parent class

        public Output(Actor.Driver<? extends Processor<?, PACKET, ?, ?>> processor,
                      Class<? extends Reactive> reactiveClass, int scopedId) {
            super(processor, reactiveClass, scopedId);
            this.processor = processor;
        }

        @Override
        public Actor.Driver<? extends Processor<?, PACKET, ?, ?>> processor() {
            return processor;
        }

        @Override
        public Identifier identifier() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ReactiveIdentifier.Output<?> output = (ReactiveIdentifier.Output<?>) o;
            return processor.equals(output.processor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), processor);
        }
    }

    public static class Input<PACKET> extends ReactiveIdentifier implements Identifier.Input<PACKET> {

        private final Actor.Driver<? extends Processor<PACKET, ?, ?, ?>> processor;  //TODO: Duplicates field from parent class

        public Input(Actor.Driver<? extends Processor<PACKET, ?, ?, ?>> processor,
                     Class<? extends Reactive> reactiveClass, int scopedId) {
            super(processor, reactiveClass, scopedId);
            this.processor = processor;
        }

        @Override
        public Actor.Driver<? extends Processor<PACKET, ?, ?, ?>> processor() {
            return processor;
        }

        @Override
        public Identifier identifier() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ReactiveIdentifier.Input<?> input = (ReactiveIdentifier.Input<?>) o;
            return processor.equals(input.processor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), processor);
        }
    }
}
