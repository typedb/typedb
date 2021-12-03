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

import java.util.function.Supplier;

public interface Pullable<T> {
    void pull(Receiver<T> receiver);

    class Source<INPUT> implements Pullable<INPUT> {

        private final Supplier<FunctionalIterator<INPUT>> iteratorSupplier;
        private FunctionalIterator<INPUT> iterator;

        public Source(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
            this.iterator = null;
        }

        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            return new Source<>(iteratorSupplier);
        }

        @Override
        public void pull(Receiver<INPUT> receiver) {
            if (iterator == null) iterator = iteratorSupplier.get();
            if (iterator.hasNext()) {
                receiver.receive(this, iterator.next());
            }
        }
    }
}
