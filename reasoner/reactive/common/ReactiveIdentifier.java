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

package com.vaticle.typedb.core.reasoner.reactive.common;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.reactive.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;

import java.util.Objects;

public class ReactiveIdentifier<P_IN, P_OUT> implements Reactive.Identifier<P_IN, P_OUT> {
    private final Actor.Driver<? extends AbstractProcessor<P_IN, P_OUT, ?, ?>> processor;
    private final Reactive reactive;
    private final long scopedId;

    public ReactiveIdentifier(Actor.Driver<? extends AbstractProcessor<P_IN, P_OUT, ?, ?>> processor,
                              Reactive reactive, long scopedId) {
        this.processor = processor;
        this.reactive = reactive;
        this.scopedId = scopedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReactiveIdentifier<?, ?> that = (ReactiveIdentifier<?, ?>) o;
        return scopedId == that.scopedId &&
                processor.equals(that.processor) &&
                reactive.equals(that.reactive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processor, reactive, scopedId);
    }

    @Override
    public String toString() {
        return "@" + Integer.toHexString(processor().hashCode()) + ":" + reactive.toString() +":" + scopedId;
    }

    @Override
    public Actor.Driver<? extends AbstractProcessor<P_IN, P_OUT, ?, ?>> processor() {
        return processor;
    }

}
