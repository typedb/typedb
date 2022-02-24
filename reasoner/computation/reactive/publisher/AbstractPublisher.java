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

package com.vaticle.typedb.core.reasoner.computation.reactive.publisher;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.BufferReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.DeduplicationReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FindFirstReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FlatMapOrRetryReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.MapReactive;

import java.util.function.Function;

public abstract class AbstractPublisher<OUTPUT> implements Reactive.Provider.Publisher<OUTPUT> {

    private final Monitoring monitor;
    private final String groupName;

    protected AbstractPublisher(Monitoring monitor, String groupName) {
        this.monitor = monitor;
        this.groupName = groupName;
    }

    protected abstract ReceiverRegistry<OUTPUT> receiverRegistry();

    protected Monitoring monitor() {
        return monitor;
    }

    @Override
    public String groupName() {
        return groupName;
    }

    @Override
    public Stream<OUTPUT,OUTPUT> findFirst() {
        FindFirstReactive<OUTPUT> findFirst = new FindFirstReactive<>(this, monitor, groupName());
        publishTo(findFirst);
        return findFirst;
    }

    @Override
    public <R> Stream<OUTPUT, R> map(Function<OUTPUT, R> function) {
        MapReactive<OUTPUT, R> map = new MapReactive<>(this, function, monitor(), groupName());
        publishTo(map);
        return map;
    }

    @Override
    public <R> Stream<OUTPUT,R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        FlatMapOrRetryReactive<OUTPUT, R> flatMap = new FlatMapOrRetryReactive<>(this, function, monitor(), groupName());
        publishTo(flatMap);
        return flatMap;
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        BufferReactive<OUTPUT> buffer = new BufferReactive<>(this, monitor(), groupName());
        publishTo(buffer);
        return buffer;
    }

    @Override
    public Stream<OUTPUT,OUTPUT> deduplicate() {
        DeduplicationReactive<OUTPUT> dedup = new DeduplicationReactive<>(this, monitor(), groupName());
        publishTo(dedup);
        return dedup;
    }
}
