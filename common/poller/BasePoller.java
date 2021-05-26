package com.vaticle.typedb.core.common.poller;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Optional;

public class BasePoller<T> extends AbstractPoller<T> {

    private final FunctionalIterator<T> source;

    public BasePoller(FunctionalIterator<T> iterator) {
        source = iterator;
    }

    @Override
    public Optional<T> poll() {
        if (source.hasNext()) return Optional.of(source.next());
        source.recycle(); // TODO: Can we call hasNext() after recycle()?
        return Optional.empty();
    }

    @Override
    public void recycle() {
        source.recycle();
    }
}
