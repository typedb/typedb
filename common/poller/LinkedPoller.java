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
