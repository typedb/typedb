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

package com.vaticle.typedb.core.reasoner.computation.reactive.provider;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.BufferedStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.DeduplicationStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FlatMapStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.MapStream;

import java.util.function.Function;

public abstract class AbstractPublisher<OUTPUT> implements Publisher<OUTPUT> {

    private final Identifier<?, ?> identifier;
    private final Processor<?, ?, ?, ?> processor;

    protected AbstractPublisher(Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
        this.identifier = this.processor.registerReactive(this);
    }

    @Override
    public Identifier<?, ?> identifier() {
        return identifier;
    }

    protected abstract ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry();

    protected Processor<?, ?, ?, ?> processor() {
        return processor;
    }

    @Override
    public <R> Stream<OUTPUT, R> map(Function<OUTPUT, R> function) {
        MapStream<OUTPUT, R> map = new MapStream<>(this, function, processor());
        registerReceiver(map);
        return map;
    }

    @Override
    public <R> Stream<OUTPUT,R> flatMap(Function<OUTPUT, FunctionalIterator<R>> function) {
        FlatMapStream<OUTPUT, R> flatMap = new FlatMapStream<>(this, function, processor());
        registerReceiver(flatMap);
        return flatMap;
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        BufferedStream<OUTPUT> buffer = new BufferedStream<>(this, processor());
        registerReceiver(buffer);
        return buffer;
    }

    @Override
    public Stream<OUTPUT,OUTPUT> deduplicate() {
        DeduplicationStream<OUTPUT> dedup = new DeduplicationStream<>(this, processor());
        registerReceiver(dedup);
        return dedup;
    }
}
