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

package com.vaticle.typedb.core.common.poller;

import java.util.Optional;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.list;

public abstract class AbstractPoller<T> implements Poller<T> {

    @Override
    public Optional<T> poll() {
        return Optional.empty();
    }

    @Override
    public <U> Poller<U> flatMap(Function<T, Poller<U>> mappingFn) {
        return new FlatMappedPoller<>(this, mappingFn);
    }

    @Override
    public Poller<T> link(Poller<T> poller) {
        return new LinkedPollers<>(list(this, poller));
    }

}
