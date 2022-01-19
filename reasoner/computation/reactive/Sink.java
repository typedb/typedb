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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.reasoner.utils.Tracer;

import javax.annotation.Nullable;

public abstract class Sink<PACKET> implements Receiver.Subscriber<PACKET>, Provider<PACKET>  {

    private Provider<PACKET> publisher;
    protected boolean isPulling;

    @Override
    public void subscribeTo(Provider<PACKET> publisher) {
        assert this.publisher == null;
        this.publisher = publisher;
        if (isPulling) publisher.pull(this);
    }

    @Override
    public void pull(@Nullable Receiver<PACKET> receiver) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        assert receiver == null;
        isPulling = true;
        if (publisher != null) publisher.pull(this);
    }

    public void pull() {
        // TODO: Block until the publisher is set with subscribeTo. rename blockingPull.
        //  Actually we can not enforce that the publisher must be present. When subscribeTo is called then pulling will start
        pull(null);
    }

}
