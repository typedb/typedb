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
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.iid.InfixIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Storage {

    boolean isOpen();

    ByteArray get(Key key);

    <T extends Key> T getLastKey(Key.Prefix<T> key);

    void deleteUntracked(Key key);

    <T extends Key> FunctionalIterator.Sorted.Forwardable<KeyValue<T, ByteArray>> iterate(Key.Prefix<T> key);

    void putUntracked(Key key);

    void putUntracked(Key key, ByteArray value);

    TypeDBException exception(ErrorMessage error);

    TypeDBException exception(Exception exception);

    void close();

    default boolean isSchema() {
        return false;
    }

    default Schema asSchema() {
        throw exception(TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class)));
    }

    interface Schema extends Storage {

        KeyGenerator.Schema schemaKeyGenerator();

        default boolean isSchema() {
            return true;
        }

        default Schema asSchema() {
            return this;
        }
    }

    interface Data extends Storage {

        KeyGenerator.Data dataKeyGenerator();

        void putTracked(Key key);

        void putTracked(Key key, ByteArray value);

        void putTracked(Key key, ByteArray value, boolean checkConsistency);

        void deleteTracked(Key key);

        void mergeUntracked(Key key, ByteArray value);

        // TODO: investigate why replacing ByteArray with Key for tracking makes navigable set intersection super slow
        void trackModified(ByteArray key);

        void trackModified(ByteArray key, boolean checkConsistency);

        void trackExclusiveBytes(ByteArray bytes);
    }

    interface Key extends Comparable<Key> {

        enum Partition {
            DEFAULT(null),
            VARIABLE_START_EDGE(null),
            FIXED_START_EDGE(VertexIID.Thing.DEFAULT_LENGTH + InfixIID.Thing.DEFAULT_LENGTH + VertexIID.Thing.PREFIX_W_TYPE_LENGTH),
            OPTIMISATION_EDGE(VertexIID.Thing.DEFAULT_LENGTH + InfixIID.Thing.RolePlayer.LENGTH + VertexIID.Thing.PREFIX_W_TYPE_LENGTH),
            STATISTICS(null);

            private final Integer fixedStartBytes;

            Partition(@Nullable Integer fixedStartBytes) {
                this.fixedStartBytes = fixedStartBytes;
            }

            public Optional<Integer> fixedStartBytes() {
                return Optional.ofNullable(fixedStartBytes);
            }
        }

        ByteArray bytes();

        Partition partition();

        @Override
        default int compareTo(Key other) {
            int compare = partition().compareTo(other.partition());
            if (compare == 0) return bytes().compareTo(other.bytes());
            else return compare;
        }

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        @Override
        String toString();

        interface Builder<K extends Key> {

            K build(ByteArray bytes);
        }

        class Prefix<K extends Key> implements Key {

            private final ByteArray prefix;
            private final Partition partition;
            private final Builder<K> builder;
            private int hash = 0;

            public Prefix(ByteArray prefix, Partition partition, Builder<K> builder) {
                this.prefix = prefix;
                this.partition = partition;
                this.builder = builder;
            }

            @Override
            public ByteArray bytes() {
                return prefix;
            }

            @Override
            public Partition partition() {
                return partition;
            }

            public boolean isFixedStartInPartition() {
                return partition.fixedStartBytes != null && prefix.length() >= partition.fixedStartBytes;
            }

            public Builder<K> builder() {
                return builder;
            }

            @Override
            public boolean equals(Object object) {
                if (this == object) return true;
                if (object == null || getClass() != object.getClass()) return false;
                Prefix<?> other = (Prefix<?>) object;
                return partition.equals(other.partition) && prefix.equals(other.prefix);
            }

            @Override
            public int hashCode() {
                if (hash == 0) hash = Objects.hash(partition, prefix);
                return hash;
            }

            @Override
            public String toString() {
                return "[" + prefix.length() + ": " + prefix.toHexString() + "][partition: " + partition + "]";
            }
        }
    }
}
