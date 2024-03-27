/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.poller;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;

public class Pollers {

    public static <T> Poller<T> empty() {
        return poll(Iterators.empty());
    }

    public static <T> Poller<T> poll(FunctionalIterator<T> iterator) {
        return new BasePoller<>(iterator);
    }
}
