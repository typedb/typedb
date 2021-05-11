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

package com.vaticle.typedb.core.graph.common;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.function.BiFunction;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Storage {

    boolean isOpen();

    ByteArray get(ByteArray key);

    ByteArray getLastKey(ByteArray prefix);

    void deleteUntracked(ByteArray key);

    void putUntracked(ByteArray key);

    void putUntracked(ByteArray key, ByteArray value);

    <G extends Bytes.ByteComparable<G>> FunctionalIterator.Sorted<G> iterate(ByteArray key, BiFunction<ByteArray, ByteArray, G> constructor);

    TypeDBException exception(ErrorMessage error);

    TypeDBException exception(Exception exception);

    void close();

    default boolean isSchema() { return false; }

    default Schema asSchema() {
        throw exception(TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class)));
    }

    class SortedPair<T extends Bytes.ByteComparable<T>, U> implements Bytes.ByteComparable<SortedPair<T, U>> {

        T first;
        U second;

        public SortedPair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T first() { return first; }

        public U second() { return second; }

        @Override
        public ByteArray getBytes() {
            return first.getBytes();
        }
    }

    interface Schema extends Storage {

        KeyGenerator.Schema schemaKeyGenerator();

        default boolean isSchema() { return true; }

        default Schema asSchema() { return this; }
    }

    interface Data extends Storage {

        KeyGenerator.Data dataKeyGenerator();

        void putTracked(ByteArray key);

        void putTracked(ByteArray key, ByteArray value);

        void putTracked(ByteArray key, ByteArray value, boolean checkConsistency);

        void deleteTracked(ByteArray key);

        void trackModified(ByteArray bytes);

        void trackModified(ByteArray bytes, boolean checkConsistency);

        void trackExclusiveCreate(ByteArray key);

        void mergeUntracked(ByteArray key, ByteArray value);

    }
}
