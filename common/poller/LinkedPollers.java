/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
