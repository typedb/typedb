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

package grakn.graph.iid;

import grakn.graph.util.Schema;

import java.util.Arrays;

import static grakn.common.collection.Bytes.join;
import static java.util.Arrays.copyOfRange;

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
        if (readableString == null) {
            readableString = "[1:" + Schema.Infix.of(bytes[0]).toString() + "]";
            if (bytes.length > 1) {
                readableString += "[" + (bytes.length - 1) + ": " +
                        Arrays.toString(copyOfRange(bytes, 1, bytes.length)) + "]";
            }
        }
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

        static InfixIID.Thing extract(byte[] bytes, int from) {
            Schema.Edge.Thing schema = Schema.Edge.Thing.of(bytes[from]);
            if ((schema.equals(Schema.Edge.Thing.ROLEPLAYER))) {
                return RolePlayer.extract(bytes, from);
            } else {
                return new InfixIID.Thing(new byte[]{bytes[from]});
            }
        }

        public static InfixIID.Thing of(Schema.Infix infix) {
            if (Schema.Edge.Thing.of(infix).equals(Schema.Edge.Thing.ROLEPLAYER)) {
                return new InfixIID.RolePlayer(infix.bytes());
            } else {
                return new InfixIID.Thing(infix.bytes());
            }
        }

        public static InfixIID.Thing of(Schema.Infix infix, IID... tail) {
            byte[][] iidBytes = new byte[tail.length + 1][];
            iidBytes[0] = infix.bytes();
            for (int i = 0; i < tail.length; i++) {
                iidBytes[i + 1] = tail[i].bytes();
            }

            if (Schema.Edge.Thing.of(infix).equals(Schema.Edge.Thing.ROLEPLAYER)) {
                return new InfixIID.RolePlayer(join(iidBytes));
            } else {
                return new InfixIID.Thing(join(iidBytes));
            }
        }

        @Override
        Schema.Edge.Thing schema() {
            return Schema.Edge.Thing.of(bytes[0]);
        }

        public InfixIID.Thing outwards() {
            if (isOutwards()) return this;
            byte[] copy = Arrays.copyOf(bytes, bytes.length);
            copy[0] = schema().out().key();
            return new InfixIID.Thing(copy);
        }

        public InfixIID.Thing inwards() {
            if (!isOutwards()) return this;
            byte[] copy = Arrays.copyOf(bytes, bytes.length);
            copy[0] = schema().in().key();
            return new InfixIID.Thing(copy);
        }

        public InfixIID.RolePlayer asRolePlayer() {
            if (this instanceof InfixIID.RolePlayer) return (InfixIID.RolePlayer) this;
            else assert false;
            return null;
        }
    }

    public static class RolePlayer extends InfixIID.Thing {

        private RolePlayer(byte[] bytes) {
            super(bytes);
        }

        public static RolePlayer of(Schema.Infix infix, VertexIID.Type type) {
            assert type != null && Schema.Edge.Thing.of(infix).equals(Schema.Edge.Thing.ROLEPLAYER);
            return new RolePlayer(join(infix.bytes(), type.bytes()));
        }

        static RolePlayer extract(byte[] bytes, int from) {
            return new RolePlayer(join(new byte[]{bytes[from]}, VertexIID.Type.extract(bytes, from + SCHEMA_LENGTH).bytes()));
        }

        public VertexIID.Type tail() {
            return VertexIID.Type.extract(bytes, SCHEMA_LENGTH);
        }
    }
}
