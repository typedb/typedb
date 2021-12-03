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

package com.vaticle.typedb.core.reasoner.stream.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Set;
import java.util.function.Function;

public class FlatMapOrRetryReactive<INPUT, OUTPUT> extends Reactive<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;

    FlatMapOrRetryReactive(Set<Receiver<OUTPUT>> downstreams, Set<Pullable<INPUT>> upstreams,
                           Function<INPUT, FunctionalIterator<OUTPUT>> transform) {
        super(downstreams, upstreams);
        this.transform = transform;
    }

    @Override
    public void receive(Pullable<INPUT> upstream, INPUT packet) {
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        if (transformed.hasNext()) {
            transformed.forEachRemaining(t -> downstreams().forEach(downstream -> downstreamReceive(downstream, t)));
            isPulling = false;
        } else if (isPulling) {
            upstreamPull(upstream);  // Automatic retry
        }
    }

}
