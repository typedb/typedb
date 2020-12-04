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

package grakn.core.graph.iid;

import grakn.core.graph.util.Encoding;

import java.util.Arrays;

import static grakn.core.common.collection.Bytes.join;
import static java.util.Arrays.copyOfRange;

public abstract class InfixIID<EDGE_ENCODING extends Encoding.Edge> extends IID {

    static final int LENGTH = 1;

    private InfixIID(byte[] bytes) {
        super(bytes);
    }

    abstract EDGE_ENCODING encoding();

    boolean isOutwards() {
        return Encoding.Edge.isOut(bytes[0]);
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public String toString() { // TODO
        if (readableString == null) {
            readableString = "[1:" + Encoding.Infix.of(bytes[0]).toString() + "]";
            if (bytes.length > 1) {
                readableString += "[" + (bytes.length - 1) + ": " +
                        Arrays.toString(copyOfRange(bytes, 1, bytes.length)) + "]";
            }
        }
        return readableString;
    }

    public static class Type extends InfixIID<Encoding.Edge.Type> {

        private Type(byte[] bytes) {
            super(bytes);
            assert bytes.length == LENGTH;
        }

        static Type of(Encoding.Infix infix) {
            return new Type(infix.bytes());
        }

        static Type extract(byte[] bytes, int from) {
            return new Type(new byte[]{bytes[from]});
        }

        @Override
        Encoding.Edge.Type encoding() {
            return Encoding.Edge.Type.of(bytes[0]);
        }
    }

    public static class Thing extends InfixIID<Encoding.Edge.Thing> {

        private Thing(byte[] bytes) {
            super(bytes);
        }

        static InfixIID.Thing extract(byte[] bytes, int from) {
            final Encoding.Edge.Thing encoding = Encoding.Edge.Thing.of(bytes[from]);
            if ((encoding.equals(Encoding.Edge.Thing.ROLEPLAYER))) {
                return RolePlayer.extract(bytes, from);
            } else {
                return new InfixIID.Thing(new byte[]{bytes[from]});
            }
        }

        public static InfixIID.Thing of(Encoding.Infix infix) {
            if (Encoding.Edge.Thing.of(infix).equals(Encoding.Edge.Thing.ROLEPLAYER)) {
                return new InfixIID.RolePlayer(infix.bytes());
            } else {
                return new InfixIID.Thing(infix.bytes());
            }
        }

        public static InfixIID.Thing of(Encoding.Infix infix, IID... tail) {
            final byte[][] iidBytes = new byte[tail.length + 1][];
            iidBytes[0] = infix.bytes();
            for (int i = 0; i < tail.length; i++) {
                iidBytes[i + 1] = tail[i].bytes();
            }

            if (Encoding.Edge.Thing.of(infix).equals(Encoding.Edge.Thing.ROLEPLAYER)) {
                return new InfixIID.RolePlayer(join(iidBytes));
            } else {
                return new InfixIID.Thing(join(iidBytes));
            }
        }

        @Override
        Encoding.Edge.Thing encoding() {
            return Encoding.Edge.Thing.of(bytes[0]);
        }

        public InfixIID.Thing outwards() {
            if (isOutwards()) return this;
            final byte[] copy = Arrays.copyOf(bytes, bytes.length);
            copy[0] = encoding().out().key();
            return new InfixIID.Thing(copy);
        }

        public InfixIID.Thing inwards() {
            if (!isOutwards()) return this;
            final byte[] copy = Arrays.copyOf(bytes, bytes.length);
            copy[0] = encoding().in().key();
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

        public static RolePlayer of(Encoding.Infix infix, VertexIID.Type type) {
            assert type != null && Encoding.Edge.Thing.of(infix).equals(Encoding.Edge.Thing.ROLEPLAYER);
            return new RolePlayer(join(infix.bytes(), type.bytes()));
        }

        static RolePlayer extract(byte[] bytes, int from) {
            return new RolePlayer(join(new byte[]{bytes[from]}, VertexIID.Type.extract(bytes, from + LENGTH).bytes()));
        }

        public VertexIID.Type tail() {
            return VertexIID.Type.extract(bytes, LENGTH);
        }
    }
}
