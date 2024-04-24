/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;

import java.util.Objects;

public class ReactiveIdentifier<P_IN, P_OUT> implements Reactive.Identifier {
    private final Actor.Driver<? extends AbstractProcessor<P_IN, P_OUT, ?, ?>> processor;
    private final Reactive reactive;
    private final long scopedId;
    private int hash = 0;

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
        if (hash == 0) hash = Objects.hash(processor, reactive, scopedId);
        return hash;
    }

    @Override
    public String toString() {
        return "@" + Integer.toHexString(processor.hashCode()) + ":" + reactive.toString() + ":" + scopedId;
    }

}
