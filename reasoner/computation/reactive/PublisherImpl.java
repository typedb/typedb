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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.function.Function;

public abstract class PublisherImpl<OUTPUT> implements Reactive.Provider.Publisher<OUTPUT> {

    protected Reactive.Receiver<OUTPUT> subscriber;
    private final PacketMonitor monitor;
    private final String groupName;

    protected PublisherImpl(PacketMonitor monitor, String groupName) {
        this.monitor = monitor;
        this.groupName = groupName;
    }

    protected PacketMonitor monitor() {
        return monitor;
    }

    @Override
    public String groupName() {
        return groupName;
    }

    @Override
    public ReactiveStream<OUTPUT, OUTPUT> findFirst() {
        FindFirstReactive<OUTPUT> findFirst = new FindFirstReactive<>(this, monitor, groupName());
        publishTo(findFirst);
        return findFirst;
    }

    @Override
    public <R> ReactiveStream<OUTPUT, R> map(Function<OUTPUT, R> function) {
        MapReactive<OUTPUT, R> map = new MapReactive<>(this, function, monitor(), groupName());
        publishTo(map);
        return map;
    }

    @Override
    public <R> ReactiveStream<OUTPUT, R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        FlatMapOrRetryReactive<OUTPUT, R> flatMap = new FlatMapOrRetryReactive<>(this, function, monitor(), groupName());
        publishTo(flatMap);
        return flatMap;
    }

    @Override
    public ReactiveStream<OUTPUT, OUTPUT> buffer() {
        BufferReactive<OUTPUT> buffer = new BufferReactive<>(this, monitor(), groupName());
        publishTo(buffer);
        return buffer;
    }

    public ReactiveStream<OUTPUT,OUTPUT> deduplicate() {
        DeduplicationReactive<OUTPUT> dedup = new DeduplicationReactive<>(this, monitor(), groupName());
        publishTo(dedup);
        return dedup;
    }
}
