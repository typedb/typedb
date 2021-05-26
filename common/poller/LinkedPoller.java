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

public class LinkedPoller<T> extends AbstractPoller<T> {

    private final Poller<T> source;
    private final Poller<T> toLink;

    public LinkedPoller(Poller<T> source, Poller<T> toLink) {
        this.source = source;
        this.toLink = toLink;
    }

    @Override
    public Optional<T> poll() {
        return source.poll().map(Optional::of).orElseGet(toLink::poll);
    }

    @Override
    public void recycle() {
        toLink.recycle();
        source.recycle();
    }
}
