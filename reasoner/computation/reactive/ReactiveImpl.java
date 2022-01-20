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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class ReactiveImpl<INPUT, OUTPUT> implements Reactive<INPUT, OUTPUT> {

    private final Set<Provider<INPUT>> publishers;
    private final PacketMonitor monitor;
    private final String groupName;
    private boolean hasForked;

    protected ReactiveImpl(Set<Publisher<INPUT>> publishers, PacketMonitor monitor, String groupName) {
        this.publishers = new HashSet<>();
        publishers.forEach(pub -> pub.publishTo(this));
        this.monitor = monitor;
        this.groupName = groupName;
        this.hasForked = false;
    }

    public PacketMonitor monitor() {
        return monitor;
    }

    @Override
    public void subscribeTo(Provider<INPUT> publisher) {
        publishers.add(publisher);
        if (isPulling()) pullFromPublisher(publisher);
    }

    protected void pullFromAllPublishers() {
        publishers.forEach(this::pullFromPublisher);
    }

    private void pullFromPublisher(Provider<INPUT> publisher) {
        monitor().onPathFork(1);
        publisher.pull(this);
        if (!hasForked) monitor().onPathTerminate();
        hasForked = true;
    }

    protected abstract boolean isPulling();

    @Override
    public String groupName() {
        return groupName;
    }

    @Override
    public ReactiveBase<OUTPUT, OUTPUT> findFirst() {
        return new FindFirstReactive<>(set(this), monitor, groupName());
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> map(Function<OUTPUT, R> function) {
        return new MapReactive<>(set(this), function, monitor, groupName());
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        return new FlatMapOrRetryReactive<>(set(this), function, monitor, groupName());
    }

}
