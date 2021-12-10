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

import java.util.Map;
import java.util.Set;

public class BufferReactive<INPUT, OUTPUT> extends Reactive<INPUT, OUTPUT> {

    Map<Subscriber<OUTPUT>, Integer> bufferPositions;

    protected BufferReactive(Set<Publisher<INPUT>> publishers) {
        super(publishers);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT packet) {

    }

    @Override
    public void publishTo(Subscribing<OUTPUT> subscriber) {
        super.publishTo(subscriber);
    }
}
