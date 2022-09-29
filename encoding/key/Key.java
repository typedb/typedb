/*
 * Copyright (C) 2022 Vaticle
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
 */

package com.vaticle.typedb.core.encoding.key;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.InfixIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;

public interface Key extends Comparable<Key> {

    enum Partition {
        DEFAULT(Encoding.Partition.DEFAULT, null),
        VARIABLE_START_EDGE(Encoding.Partition.VARIABLE_START_EDGE, null),
        FIXED_START_EDGE(Encoding.Partition.FIXED_START_EDGE, VertexIID.Thing.DEFAULT_LENGTH + InfixIID.Thing.DEFAULT_LENGTH + VertexIID.Thing.PREFIX_W_TYPE_LENGTH),
        OPTIMISATION_EDGE(Encoding.Partition.OPTIMISATION_EDGE, VertexIID.Thing.DEFAULT_LENGTH + InfixIID.Thing.RolePlayer.LENGTH + VertexIID.Thing.PREFIX_W_TYPE_LENGTH),
        METADATA(Encoding.Partition.METADATA, null);

        private final Encoding.Partition encoding;
        private final Integer fixedStartBytes;

        public static Key.Partition fromID(byte ID) {
            if (ID == Encoding.Partition.DEFAULT.ID()) {
                return DEFAULT;
            } else if (ID == Encoding.Partition.VARIABLE_START_EDGE.ID()) {
                return VARIABLE_START_EDGE;
            } else if (ID == Encoding.Partition.FIXED_START_EDGE.ID()) {
                return FIXED_START_EDGE;
            } else if (ID == Encoding.Partition.OPTIMISATION_EDGE.ID()) {
                return OPTIMISATION_EDGE;
            } else if (ID == Encoding.Partition.METADATA.ID()) {
                return METADATA;
            } else {
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        Partition(Encoding.Partition encoding, @Nullable Integer fixedStartBytes) {
            this.encoding = encoding;
            this.fixedStartBytes = fixedStartBytes;
        }

        public Encoding.Partition encoding() {
            return encoding;
        }

        public Optional<Integer> fixedStartBytes() {
            return Optional.ofNullable(fixedStartBytes);
        }
    }

    ByteArray bytes();

    Key.Partition partition();

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
        private final Key.Partition partition;
        private final Key.Builder<K> builder;
        private int hash = 0;

        public Prefix(ByteArray prefix, Key.Partition partition, Key.Builder<K> builder) {
            this.prefix = prefix;
            this.partition = partition;
            this.builder = builder;
        }

        @Override
        public ByteArray bytes() {
            return prefix;
        }

        @Override
        public Key.Partition partition() {
            return partition;
        }

        public boolean isFixedStartInPartition() {
            return partition.fixedStartBytes != null && prefix.length() >= partition.fixedStartBytes;
        }

        public Key.Builder<K> builder() {
            return builder;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Key.Prefix<?> other = (Key.Prefix<?>) object;
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
