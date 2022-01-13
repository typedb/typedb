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

import java.util.function.Consumer;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class ReactiveImpl<INPUT, OUTPUT> implements Reactive<INPUT, OUTPUT> {

    private final String groupName;

    protected ReactiveImpl(String groupName) {this.groupName = groupName;}

    @Override
    public String groupName() {
        return groupName;
    }

    @Override
    public ReactiveBase<OUTPUT, OUTPUT> findFirst() {
        return new FindFirstReactive<>(set(this), groupName());
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> map(Function<OUTPUT, R> function) {
        return new MapReactive<>(set(this), function, groupName());
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        return new FlatMapOrRetryReactive<>(set(this), function, groupName());
    }

    @Override
    public void forEach(Consumer<OUTPUT> function) {
        this.publishTo(new ForEachReactive<>(function, groupName()));
    }
}
