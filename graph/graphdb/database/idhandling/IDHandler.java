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
 */

package grakn.core.graph.graphdb.database.idhandling;

import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.WriteByteBuffer;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import static grakn.core.graph.graphdb.idmanagement.IDManager.VertexIDType.SystemEdgeLabel;
import static grakn.core.graph.graphdb.idmanagement.IDManager.VertexIDType.SystemPropertyKey;
import static grakn.core.graph.graphdb.idmanagement.IDManager.VertexIDType.UserEdgeLabel;
import static grakn.core.graph.graphdb.idmanagement.IDManager.VertexIDType.UserPropertyKey;


public class IDHandler {

    public static final StaticBuffer MIN_KEY = BufferUtil.getLongBuffer(0);
    public static final StaticBuffer MAX_KEY = BufferUtil.getLongBuffer(-1);

    public enum DirectionID {

        PROPERTY_DIR(0),  //00b
        EDGE_OUT_DIR(2),  //10b
        EDGE_IN_DIR(3);   //11b

        private final int id;

        DirectionID(int id) {
            this.id = id;
        }

        private int getRelationType() {
            return id >>> 1;
        }

        private int getDirectionInt() {
            return id & 1;
        }

        public RelationCategory getRelationCategory() {
            switch (this) {
                case PROPERTY_DIR:
                    return RelationCategory.PROPERTY;
                case EDGE_IN_DIR:
                case EDGE_OUT_DIR:
                    return RelationCategory.EDGE;
                default:
                    throw new AssertionError();
            }
        }

        public Direction getDirection() {
            switch (this) {
                case PROPERTY_DIR:
                case EDGE_OUT_DIR:
                    return Direction.OUT;
                case EDGE_IN_DIR:
                    return Direction.IN;
                default:
                    throw new AssertionError();
            }
        }

        private int getPrefix(boolean invisible, boolean systemType) {
            return ((systemType ? 0 : invisible ? 2 : 1) << 1) + getRelationType();
        }

        private static DirectionID getDirectionID(int relationType, int direction) {
            return forId((relationType << 1) + direction);
        }

        private static DirectionID forId(int id) {
            switch (id) {
                case 0:
                    return PROPERTY_DIR;
                case 2:
                    return EDGE_OUT_DIR;
                case 3:
                    return EDGE_IN_DIR;
                default:
                    throw new AssertionError("Invalid id: " + id);
            }
        }
    }


    private static final int PREFIX_BIT_LEN = 3;

    private static int relationTypeLength(long relationTypeId) {
        return VariableLong.positiveWithPrefixLength(IDManager.stripEntireRelationTypePadding(relationTypeId) << 1, PREFIX_BIT_LEN);
    }

    /**
     * The edge type is written as follows: [ Invisible &amp; System (2 bit) | Relation-Type-ID (1 bit) | Relation-Type-Count (variable) | Direction-ID (1 bit)]
     * Would only need 1 bit to store relation-type-id, but using two so we can upper bound.
     */
    public static void writeRelationType(WriteBuffer out, long relationTypeId, DirectionID dirID, boolean invisible) {
        long strippedId = (IDManager.stripEntireRelationTypePadding(relationTypeId) << 1) + dirID.getDirectionInt();
        VariableLong.writePositiveWithPrefix(out, strippedId, dirID.getPrefix(invisible, IDManager.isSystemRelationTypeId(relationTypeId)), PREFIX_BIT_LEN);
    }

    public static StaticBuffer getRelationType(long relationTypeId, DirectionID dirID, boolean invisible) {
        WriteBuffer b = new WriteByteBuffer(relationTypeLength(relationTypeId));
        IDHandler.writeRelationType(b, relationTypeId, dirID, invisible);
        return b.getStaticBuffer();
    }

    public static RelationTypeParse readRelationType(ReadBuffer in) {
        long[] countPrefix = VariableLong.readPositiveWithPrefix(in, PREFIX_BIT_LEN);
        DirectionID dirID = DirectionID.getDirectionID((int) countPrefix[1] & 1, (int) (countPrefix[0] & 1));
        long typeId = countPrefix[0] >>> 1;
        boolean isSystemType = (countPrefix[1] >> 1) == 0;

        if (dirID == DirectionID.PROPERTY_DIR) {
            typeId = IDManager.getSchemaId(isSystemType ? SystemPropertyKey : UserPropertyKey, typeId);
        } else {
            typeId = IDManager.getSchemaId(isSystemType ? SystemEdgeLabel : UserEdgeLabel, typeId);
        }
        return new RelationTypeParse(typeId, dirID);
    }

    public static class RelationTypeParse {

        public final long typeId;
        public final DirectionID dirID;

        RelationTypeParse(long typeId, DirectionID dirID) {
            this.typeId = typeId;
            this.dirID = dirID;
        }
    }


    public static void writeInlineRelationType(WriteBuffer out, long relationTypeId) {
        long compressId = IDManager.stripRelationTypePadding(relationTypeId);
        VariableLong.writePositive(out, compressId);
    }

    public static long readInlineRelationType(ReadBuffer in) {
        long compressId = VariableLong.readPositive(in);
        return IDManager.addRelationTypePadding(compressId);
    }

    private static StaticBuffer getPrefixed(int prefix) {
        byte[] arr = new byte[1];
        arr[0] = (byte) (prefix << (Byte.SIZE - PREFIX_BIT_LEN));
        return new StaticArrayBuffer(arr);
    }

    public static StaticBuffer[] getBounds(RelationCategory type, boolean systemTypes) {
        int start, end;
        switch (type) {
            case PROPERTY:
                start = DirectionID.PROPERTY_DIR.getPrefix(systemTypes, systemTypes);
                end = start;
                break;
            case EDGE:
                start = DirectionID.EDGE_OUT_DIR.getPrefix(systemTypes, systemTypes);
                end = start;
                break;
            case RELATION:
                start = DirectionID.PROPERTY_DIR.getPrefix(systemTypes, systemTypes);
                end = DirectionID.EDGE_OUT_DIR.getPrefix(systemTypes, systemTypes);
                break;
            default:
                throw new AssertionError("Unrecognized type:" + type);
        }
        end++;
        return new StaticBuffer[]{getPrefixed(start), getPrefixed(end)};
    }

}
