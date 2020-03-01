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

package grakn.core.graph.diskstorage.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Utility methods for dealing with ByteBuffer.
 */
public class BufferUtil {

    private static final int longSize = StaticArrayBuffer.LONG_LEN;
    private static final int intSize = StaticArrayBuffer.INT_LEN;

    /* ###############
     * Simple StaticBuffer construction
     * ################
     */

    public static StaticBuffer getIntBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize);
        return StaticArrayBuffer.of(arr);
    }

    public static StaticBuffer getIntBuffer(int[] ids) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize * ids.length);
        for (int id : ids) buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize * ids.length);
        return StaticArrayBuffer.of(arr);
    }

    public static StaticBuffer getLongBuffer(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize);
        buffer.putLong(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == longSize);
        return StaticArrayBuffer.of(arr);
    }


    private static StaticBuffer fillBuffer(int len, byte value) {
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) res[i]=value;
        return StaticArrayBuffer.of(res);
    }

    public static StaticBuffer oneBuffer(int len) {
        return fillBuffer(len,(byte)-1);
    }

    public static StaticBuffer zeroBuffer(int len) {
        return fillBuffer(len,(byte)0);
    }

    /* ################
     * Buffer I/O
     * ################
     */

    public static void writeEntry(DataOutput out, Entry entry) {
        VariableLong.writePositive(out,entry.getValuePosition());
        writeBuffer(out,entry);
        if (!entry.hasMetaData()) out.putByte((byte)0);
        else {
            Map<EntryMetaData,Object> metadata = entry.getMetaData();
            out.putByte((byte)metadata.size());
            for (Map.Entry<EntryMetaData,Object> metas : metadata.entrySet()) {
                EntryMetaData meta = metas.getKey();
                out.putByte((byte)meta.ordinal());
                out.writeObjectNotNull(metas.getValue());
            }
        }
    }

    public static void writeBuffer(DataOutput out, StaticBuffer buffer) {
        VariableLong.writePositive(out,buffer.length());
        out.putBytes(buffer);
    }

    public static Entry readEntry(ReadBuffer in, Serializer serializer) {
        long valuePosition = VariableLong.readPositive(in);
        Preconditions.checkArgument(valuePosition>0 && valuePosition<=Integer.MAX_VALUE);
        StaticBuffer buffer = readBuffer(in);

        StaticArrayEntry entry = new StaticArrayEntry(buffer, (int) valuePosition);
        int metaSize = in.getByte();
        for (int i=0;i<metaSize;i++) {
            EntryMetaData meta = EntryMetaData.values()[in.getByte()];
            entry.setMetaData(meta,serializer.readObjectNotNull(in,meta.getDataType()));
        }
        return entry;
    }

    private static StaticBuffer readBuffer(ScanBuffer in) {
        long length = VariableLong.readPositive(in);
        Preconditions.checkArgument(length>=0 && length<=Integer.MAX_VALUE);
        byte[] data = in.getBytes((int)length);
        return new StaticArrayBuffer(data);
    }

    /* ################
     * StaticBuffer Manipulation
     * ################
     */

    public static StaticBuffer padBuffer(StaticBuffer b, int length) {
        if (b.length()>=length) return b;
        byte[] data = new byte[length]; //implicitly initialized to all 0s
        for (int i = 0; i < b.length(); i++) {
            data[i]=b.getByte(i);
        }
        return new StaticArrayBuffer(data);
    }

    public static StaticBuffer nextBiggerBufferAllowOverflow(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer, true);
    }

    public static StaticBuffer nextBiggerBuffer(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer,false);
    }

    private static StaticBuffer nextBiggerBuffer(StaticBuffer buffer, boolean allowOverflow) {
        int len = buffer.length();
        byte[] next = new byte[len];
        boolean carry = true;
        for (int i = len - 1; i >= 0; i--) {
            byte b = buffer.getByte(i);
            if (carry) {
                b++;
                if (b != 0) carry = false;
            }
            next[i]=b;
        }
        if (carry && allowOverflow) {
            return zeroBuffer(len);
        } else if (carry) {
            throw new IllegalArgumentException("Buffer overflow incrementing " + buffer);
        } else {
            return StaticArrayBuffer.of(next);
        }

    }

    /**
     * Thread safe equals method for StaticBuffer - ByteBuffer equality comparison
     */
    public static boolean equals(StaticBuffer b1, ByteBuffer b2) {
        if (b1.length()!=b2.remaining()) return false;
        int p2 = b2.position();
        for (int i=0;i<b1.length();i++) {
            if (b1.getByte(i)!=b2.get(p2+i)) return false;
        }
        return true;
    }


}
