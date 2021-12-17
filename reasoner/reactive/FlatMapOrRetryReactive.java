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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Set;
import java.util.function.Function;

public class FlatMapOrRetryReactive<INPUT, OUTPUT> extends ReactiveBase<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;

    FlatMapOrRetryReactive(Set<Publisher<INPUT>> publishers, Function<INPUT, FunctionalIterator<OUTPUT>> transform) {
        super(publishers);
        this.transform = transform;
    }

    @Override
    public void receive(Provider<INPUT> provider, INPUT packet) {
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        if (transformed.hasNext()) {
            transformed.forEachRemaining(t -> subscriber().receive(this, t));
            finishPulling();
        } else if (isPulling()) {
            provider.pull(this);  // Automatic retry
        }
    }

}
