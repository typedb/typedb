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

package com.vaticle.typedb.core.reasoner.stream.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class Reactive<INPUT, OUTPUT> implements Pullable<OUTPUT>, Receiver<INPUT> {  // TODO: Pull out an interface for Reactive

    protected final Set<Receiver<OUTPUT>> downstreams;
    private final Set<Pullable<INPUT>> upstreams;
    protected boolean isPulling;

    Reactive(Set<Receiver<OUTPUT>> downstreams, Set<Pullable<INPUT>> upstreams) {
        this.downstreams = downstreams;
        this.upstreams = upstreams;
        this.isPulling = false;
    }

    @Override
    public void pull(Receiver<OUTPUT> receiver) {
        addDownstream(receiver);  // TODO: This way we dynamically add the downstreams
        if (!isPulling) {
            upstreams.forEach(this::upstreamPull);
            isPulling = true;
        }
    }

    protected Set<Receiver<OUTPUT>> downstreams() {
        return downstreams;
    }

    protected Set<Pullable<INPUT>> upstreams() {
        return upstreams;
    }

    protected void addDownstream(Receiver<OUTPUT> downstream) {
        downstreams.add(downstream);
        // TODO: To dynamically add downstreams we need to have buffered all prior packets and send them here
        //  we can adopt a policy that if you weren't a downstream in time for the packet then you miss it, and
        //  break this only for outlets which will do the buffering and ensure all downstreams receive all answers.
    }

    public void forkTo(Receiver<OUTPUT> receiver) {
        addDownstream(receiver);
    }

    private Pullable<INPUT> addUpstream(Pullable<INPUT> upstream) {
        upstreams.add(upstream);
        if (isPulling) upstream.pull(this);
        return upstream;
    }

    public Reactive<INPUT, OUTPUT> join(Pullable<INPUT> pullable) {
        // TODO: join looks strange because all other fluent methods are also doing a join implicitly. Fix this.
        addUpstream(pullable);
        return this;
    }

    protected void downstreamReceive(Receiver<OUTPUT> downstream, OUTPUT p) {
        // TODO: Override for cross-actor receiving
        downstream.receive(this, p);  // TODO: Remove casting
    }

    protected void upstreamPull(Pullable<INPUT> upstream) {
        // TODO: Override for cross-actor pulling
        upstream.pull(this);
    }

    public IdentityReactive<INPUT> findFirstIf(boolean condition) {
        if (condition) {
            FindFirstReactive<INPUT> newReactive = new FindFirstReactive<>(set(this), set());
            addUpstream(newReactive);
            return newReactive;
        } else {
            IdentityReactive<INPUT> newReactive = new IdentityReactive<>(set(this), set());
            addUpstream(newReactive);
            return newReactive;
        }
    }

    public <UPS_INPUT> MapReactive<UPS_INPUT, INPUT> map(Function<UPS_INPUT, INPUT> function) {
        MapReactive<UPS_INPUT, INPUT> newReactive = new MapReactive<>(set(this), set(), function);
        addUpstream(newReactive);
        return newReactive;
    }

    public <UPS_INPUT> FlatMapOrRetryReactive<UPS_INPUT, INPUT> flatMapOrRetry(Function<UPS_INPUT, FunctionalIterator<INPUT>> function) {
        FlatMapOrRetryReactive<UPS_INPUT, INPUT> newReactive = new FlatMapOrRetryReactive<>(set(this), set(), function);
        addUpstream(newReactive);
        return newReactive;
    }

}
