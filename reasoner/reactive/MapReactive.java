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

import java.util.Set;
import java.util.function.Function;

public class MapReactive<INPUT, OUTPUT> extends ReactiveImpl<INPUT, OUTPUT> {

    private final Function<INPUT, OUTPUT> mappingFunc;

    protected MapReactive(Set<Subscriber<OUTPUT>> subscribers, Set<Publisher<INPUT>> publishers,
                Function<INPUT, OUTPUT> mappingFunc) {
        super(subscribers, publishers);
        this.mappingFunc = mappingFunc;
    }

    public static <I, O> MapReactive<I, O> map(Set<Subscriber<O>> subscribers, Set<Publisher<I>> publishers,
                                               Function<I, O> mappingFunc) {
        return new MapReactive<>(subscribers, publishers, mappingFunc);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT packet) {
        subscribers().forEach(subscriber -> subscriberReceive(subscriber, mappingFunc.apply(packet)));
    }

}
