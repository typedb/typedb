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
import java.util.function.Predicate;

public class FilteredPoller<T> extends AbstractPoller<T> {

    private final Poller<T> poller;
    private final Predicate<T> predicate;

    public FilteredPoller(Poller<T> poller, Predicate<T> predicate) {
        this.poller = poller;
        this.predicate = predicate;
    }

    @Override
    public Optional<T> poll() {
        Optional<T> fromSource;
        while ((fromSource = poller.poll()).isPresent()) {
            if (predicate.test(fromSource.get())) return fromSource;
        }
        return Optional.empty();
    }

    @Override
    public void recycle() {
        poller.recycle();
    }
}
