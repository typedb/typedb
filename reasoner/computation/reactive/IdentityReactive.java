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

import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;

import java.util.HashSet;
import java.util.Set;

public class IdentityReactive<PACKET> extends ReactiveBase<PACKET, PACKET> {

    protected IdentityReactive(Set<Publisher<PACKET>> publishers) {
        super(publishers);
    }

    public static <T> IdentityReactive<T> noOp() {
        return new IdentityReactive<>(new HashSet<>());
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        finishPulling();
        subscriber().receive(this, packet);
    }
}
