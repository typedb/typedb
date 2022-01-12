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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;

import java.util.function.Supplier;

public class Source<PACKET> extends PublisherImpl<PACKET> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private FunctionalIterator<PACKET> iterator;

    public Source(Supplier<FunctionalIterator<PACKET>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
        return new Source<>(iteratorSupplier);
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(subscriber);
        ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
        if (iterator == null) iterator = iteratorSupplier.get();
        if (iterator.hasNext()) receiver.receive(this, iterator.next());
    }

}
