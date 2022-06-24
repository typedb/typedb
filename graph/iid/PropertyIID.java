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
 *
 */

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;

public abstract class PropertyIID extends PartitionedIID {

    private static final Partition PARTITION = Partition.DEFAULT;

    PropertyIID(ByteArray bytes) {
        super(bytes);
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static class Type extends PropertyIID {

        private static final int LENGTH = VertexIID.Type.LENGTH + InfixIID.DEFAULT_LENGTH;

        private Type(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static Type of(VertexIID.Type typeVertex, Encoding.Property property) {
            return new Type(ByteArray.join(typeVertex.bytes, property.infix().bytes()));
        }

        public static Type extract(ByteArray bytes, int from) {
            return new Type(bytes.view(from, from + LENGTH));
        }

        public static Key.Prefix<Type> prefix(VertexIID.Type type) {
            return new Key.Prefix<>(type.bytes, PARTITION, (bytes) -> extract(bytes, 0));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = VertexIID.Type.extract(bytes, 0).toString() +
                        "[" + Encoding.Infix.LENGTH + ": " + Encoding.Infix.of(bytes.get(VertexIID.Type.LENGTH)) + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }

    public static class Structure extends PropertyIID {

        private static final int LENGTH = StructureIID.LENGTH + InfixIID.DEFAULT_LENGTH;

        private Structure(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static Structure of(StructureIID structure, Encoding.Property property) {
            return new Structure(ByteArray.join(structure.bytes, property.infix().bytes()));
        }

        public static Structure extract(ByteArray bytes, int from) {
            return new Structure(bytes.view(from, from + LENGTH));
        }

        public static Key.Prefix<Structure> prefix(StructureIID structure) {
            return new Key.Prefix<>(structure.bytes, PARTITION, (bytes) -> extract(bytes, 0));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = StructureIID.Rule.extract(bytes, 0).toString() +
                        "[" + Encoding.Infix.LENGTH + ": " + Encoding.Infix.of(bytes.get(StructureIID.Rule.LENGTH)) + "]" +
                        "[partition: " + partition() + "]";

            }
            return readableString;
        }
    }
}
