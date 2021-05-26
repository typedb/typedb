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
