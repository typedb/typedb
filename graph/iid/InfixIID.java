/*
 * Copyright (C) 2020 Grakn Labs
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

package hypergraph.graph.iid;

import hypergraph.common.collection.Bytes;
import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.util.Schema;

import static hypergraph.common.collection.Bytes.join;

public abstract class InfixIID<EDGE_SCHEMA extends Schema.Edge> extends IID {

    static final int SCHEMA_LENGTH = 1;

    private InfixIID(byte[] bytes) {
        super(bytes);
    }

    abstract EDGE_SCHEMA schema();

    boolean isOutwards() {
        return Schema.Edge.isOut(bytes[0]);
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public String toString() { // TODO
        if (readableString == null) readableString = "[" + Schema.Infix.of(bytes[0]).toString() + "]";
        return readableString;
    }

    public static class Type extends InfixIID<Schema.Edge.Type> {

        static final int LENGTH = SCHEMA_LENGTH;

        private Type(byte[] bytes) {
            super(bytes);
            assert bytes.length == LENGTH;
        }

        static InfixIID.Type of(Schema.Infix infix) {
            return new InfixIID.Type(infix.bytes());
        }

        static Type extract(byte[] bytes, int from) {
            return new InfixIID.Type(new byte[]{bytes[from]});
        }

        @Override
        Schema.Edge.Type schema() {
            return Schema.Edge.Type.of(bytes[0]);
        }
    }

    public static class Thing extends InfixIID<Schema.Edge.Thing> {

        private Thing(byte[] bytes) {
            super(bytes);
        }

        static InfixIID.Thing of(Schema.Infix infix) {
            return new InfixIID.Thing(infix.bytes());
        }

        public static InfixIID.Thing of(Schema.Infix infix, VertexIID.Type metadata) {
            return of(infix);
//            if (!infix.isOptimisation()) {
//                assert metadata == null;
//                return of(infix);
//            } else {
//                // For now, we only have OPT_ROLE as an optimisation edge
//                return InfixIID.OptimisedRole.of(infix, metadata);
//            }
        }

        static InfixIID.Thing extract(byte[] bytes, int from) {
            Schema.Edge.Thing schema = Schema.Edge.Thing.of(bytes[from]);
            if (!schema.isOptimisation()) {
                return new InfixIID.Thing(new byte[]{bytes[from]});
            } else if ((schema.equals(Schema.Edge.Thing.OPT_ROLE))) {
                return OptimisedRole.extract(bytes, from);
            } else {
                assert false;
                throw new HypergraphException(Error.Internal.UNRECOGNISED_VALUE);
            }
        }

        @Override
        Schema.Edge.Thing schema() {
            return Schema.Edge.Thing.of(bytes[0]);
        }

        public InfixIID.Thing withoutMetaData() {
            if (bytes.length == SCHEMA_LENGTH) return this;
            else return new Thing(new byte[]{bytes[0]});
        }

        public boolean isOptimisation() {
            return schema().isOptimisation();
        }

        public boolean hasMetaData() {
            return bytes.length > SCHEMA_LENGTH;
        }

        public VertexIID.Type metadata() {
            return null;
        }

        public boolean containsMetaData(VertexIID.Type metadata) {
            return false;
        }
    }

    public static class OptimisedRole extends InfixIID.Thing {

        private OptimisedRole(byte[] bytes) {
            super(bytes);
        }

        public static InfixIID.OptimisedRole of(Schema.Infix infix, VertexIID.Type type) {
            assert type != null && Schema.Edge.Thing.of(infix).equals(Schema.Edge.Thing.OPT_ROLE);
            return new InfixIID.OptimisedRole(join(infix.bytes(), type.bytes()));
        }

        static InfixIID.Thing extract(byte[] bytes, int from) {
            return new InfixIID.OptimisedRole(join(new byte[]{bytes[0]}, VertexIID.Type.extract(bytes, from + SCHEMA_LENGTH).bytes()));
        }

        public VertexIID.Type metadata() {
            return VertexIID.Type.extract(bytes, SCHEMA_LENGTH);
        }

        public boolean containsMetaData(VertexIID.Type metadata) {
            return Bytes.arrayContains(bytes, SCHEMA_LENGTH, metadata.bytes);
        }
    }
}
