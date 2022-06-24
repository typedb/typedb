/*
 * Copyright (C) 2022 Vaticle
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LinkedPollers<T> extends AbstractPoller<T> {

    private final List<Poller<T>> pollers;

    LinkedPollers(List<Poller<T>> pollers) {
        this.pollers = new ArrayList<>(pollers);
    }

    @Override
    public Optional<T> poll() {
        for (Poller<T> poller : pollers) {
            Optional<T> next = poller.poll();
            if (next.isPresent()) return next;
        }
        return Optional.empty();
    }

    @Override
    public void recycle() {
        pollers.forEach(Poller::recycle);
    }
}
