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

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.key.Key;

public abstract class PropertyIID extends PartitionedIID {

    private static final Partition PARTITION = Partition.DEFAULT;

    PropertyIID(ByteArray bytes) {
        super(bytes);
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static class TypeVertex extends PropertyIID {

        private static final int LENGTH = VertexIID.Type.LENGTH + InfixIID.DEFAULT_LENGTH;

        private TypeVertex(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static TypeVertex of(VertexIID.Type typeVertex, Encoding.Property.Vertex property) {
            return new TypeVertex(ByteArray.join(typeVertex.bytes, property.infix().bytes()));
        }

        public static TypeVertex extract(ByteArray bytes, int from) {
            return new TypeVertex(bytes.view(from, from + LENGTH));
        }

        public static Key.Prefix<TypeVertex> prefix(VertexIID.Type type) {
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

    public static class TypeEdge extends PropertyIID {

        private static final int LENGTH = VertexIID.Type.LENGTH + Encoding.Infix.LENGTH + VertexIID.Type.LENGTH;

        TypeEdge(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static TypeEdge of(VertexIID.Type start, VertexIID.Type end, Encoding.Property.Edge property) {
            return new TypeEdge(ByteArray.join(start.bytes, property.infix().bytes(), end.bytes));
        }

        public static TypeEdge extract(ByteArray bytes, int from) {
            return new TypeEdge(bytes.view(from, from + LENGTH));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = VertexIID.Type.of(bytes.view(0, VertexIID.Type.LENGTH)) +
                        "[" + Encoding.Infix.LENGTH + ": " + Encoding.Infix.of(bytes.get(EdgeViewIID.Type.LENGTH)) + "]" +
                        VertexIID.Type.of(bytes.view(VertexIID.Type.LENGTH + Encoding.Infix.LENGTH, LENGTH)) +
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

        public static Structure of(StructureIID structure, Encoding.Property.Structure property) {
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
