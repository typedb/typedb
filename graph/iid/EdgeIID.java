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

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public abstract class EdgeIID<
        EDGE_ENCODING extends Encoding.Edge,
        EDGE_INFIX extends InfixIID<EDGE_ENCODING>,
        VERTEX_IID_START extends VertexIID,
        VERTEX_IID_END extends VertexIID> extends PartitionedIID {

    EDGE_INFIX infix;
    VERTEX_IID_START start;
    VERTEX_IID_END end;
    SuffixIID suffix;
    private int endIndex, infixIndex, suffixIndex;

    EdgeIID(ByteArray bytes) {
        super(bytes);
    }

    abstract EDGE_INFIX infix();

    abstract VERTEX_IID_START start();

    abstract VERTEX_IID_END end();

    int infixIndex() {
        if (infixIndex == 0) infixIndex = start().bytes.length();
        return infixIndex;
    }

    int endIndex() {
        if (endIndex == 0) endIndex = infixIndex() + infix().bytes.length();
        return endIndex;
    }

    int suffixIndex() {
        if (suffixIndex == 0) suffixIndex = endIndex() + end().bytes.length();
        return suffixIndex;
    }

    public EDGE_ENCODING encoding() {
        return infix().encoding();
    }

    // TODO: rename to isForward()
    public boolean isOutwards() {
        return infix().isOutwards();
    }

    @Override
    public String toString() {
        if (readableString == null) {
            readableString = "[" + start().bytes.length() + ": " + start().toString() + "]" +
                    "[" + infix().length() + ": " + infix().toString() + "]" +
                    "[" + end().bytes.length() + ": " + end().toString() + "]" +
                    "[partition:" + partition() + "]";
        }
        return readableString;
    }

    public static class Type extends EdgeIID<Encoding.Edge.Type, InfixIID.Type, VertexIID.Type, VertexIID.Type> {

        private static final int LENGTH = 2 * VertexIID.Type.LENGTH + InfixIID.Type.DEFAULT_LENGTH;

        Type(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static Type of(ByteArray bytes) {
            return new Type(bytes);
        }

        public static Type of(VertexIID.Type start, Encoding.Infix infix, VertexIID.Type end) {
            return new Type(join(start.bytes, infix.bytes(), end.bytes));
        }

        public static Key.Prefix<EdgeIID.Type> prefix(VertexIID.Type vertex, InfixIID<?> infixIID) {
            return new Key.Prefix<>(join(vertex.bytes, infixIID.bytes), Partition.DEFAULT, Type::of);
        }

        @Override
        public InfixIID.Type infix() {
            if (infix == null) infix = InfixIID.Type.extract(bytes, VertexIID.Type.LENGTH);
            return infix;
        }

        @Override
        public VertexIID.Type start() {
            if (start == null) start = VertexIID.Type.of(bytes.view(0, VertexIID.Type.LENGTH));
            return start;
        }

        @Override
        public VertexIID.Type end() {
            if (end != null) return end;
            end = VertexIID.Type.of(bytes.view(bytes.length() - VertexIID.Type.LENGTH, bytes.length()));
            return end;
        }

        @Override
        public Partition partition() {
            return Partition.DEFAULT;
        }
    }

    public static class Thing extends EdgeIID<Encoding.Edge.Thing, InfixIID.Thing, VertexIID.Thing, VertexIID.Thing> {

        Thing(ByteArray bytes) {
            super(bytes);
        }

        public static Thing of(ByteArray bytes) {
            return new Thing(bytes);
        }

        public static Thing of(VertexIID.Thing start, InfixIID.Thing infix, VertexIID.Thing end) {
            return new Thing(join(start.bytes, infix.bytes, end.bytes));
        }

        public static Thing of(VertexIID.Thing start, InfixIID.Thing infix, VertexIID.Thing end, SuffixIID suffix) {
            return new Thing(join(start.bytes, infix.bytes, end.bytes, suffix.bytes));
        }

        public static Key.Prefix<Thing> prefix(VertexIID.Thing start, InfixIID.Thing infix) {
            return new Key.Prefix<>(join(start.bytes, infix.bytes), computePartition(start, infix), Thing::of);
        }

        @Override
        public InfixIID.Thing infix() {
            if (infix == null) infix = InfixIID.Thing.extract(bytes, infixIndex());
            return infix;
        }

        public SuffixIID suffix() {
            if (suffix == null) {
                if (suffixIndex() >= bytes.length()) suffix = SuffixIID.of(ByteArray.empty());
                else suffix = SuffixIID.of(bytes.view(suffixIndex()));
            }
            return suffix;
        }

        @Override
        public VertexIID.Thing start() {
            if (start == null) start = VertexIID.Thing.extract(bytes, 0);
            return start;
        }

        @Override
        public VertexIID.Thing end() {
            if (end == null) end = VertexIID.Thing.extract(bytes, endIndex());
            return end;
        }

        @Override
        public Partition partition() {
            return computePartition(start(), infix());
        }

        static Partition computePartition(VertexIID.Thing start, InfixIID.Thing infix) {
            if (start.isAttribute()) return Partition.VARIABLE_START_EDGE;
            else if (infix.isRolePlayer()) return Partition.OPTIMISATION_EDGE;
            else return Partition.FIXED_START_EDGE;
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = super.toString();
                if (!suffix().isEmpty()) {
                    readableString += "[" + suffix().bytes.length() + ": " + suffix().toString() + "]";
                }
                readableString += "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }
}
