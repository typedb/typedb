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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;


public class VariableLong {

    public static int unsignedByte(byte b) {
        return b < 0 ? b + 256 : b;
    }

    public static byte unsignedByte(int value) {
        Preconditions.checkArgument(value >= 0 && value < 256, "Value overflow: %s", value);
        return (byte) (value & 0xFF);
    }

    //Move stop bit back to front => rewrite prefix variable encoding by custom writing first byte

    private static final byte BIT_MASK = 127;
    private static final byte STOP_MASK = -128;

    private static long readUnsigned(ScanBuffer in) {
        long value = 0;
        byte b;
        do {
            b = in.getByte();
            value = value << 7 | (b & BIT_MASK);
        } while (b >= 0);
        return value;
    }


    private static void writeUnsigned(WriteBuffer out, long value) {
        writeUnsigned(out, unsignedBlockBitLength(value), value);
    }

    private static void writeUnsigned(WriteBuffer out, int offset, long value) {
        while (offset > 0) {
            offset -= 7;
            byte b = (byte) ((value >>> offset) & BIT_MASK);
            if (offset == 0) {
                b = (byte) (b | STOP_MASK);
            }
            out.putByte(b);
        }
    }

    private static int unsignedBlockBitLength(long value) {
        return unsignedNumBlocks(value) * 7;
    }

    private static int unsignedNumBlocks(long value) {
        return numVariableBlocks(unsignedBitLength(value));
    }

    private static int numVariableBlocks(int numBits) {
        return (numBits - 1) / 7 + 1;
    }

    public static int unsignedBitLength(long value) {
        return (value == 0) ? 1 : Long.SIZE - Long.numberOfLeadingZeros(value);
    }

    /* ##################################
          Read and write positive longs
       ################################## */


    public static long readPositive(ScanBuffer in) {
        return readUnsigned(in);
    }

    public static void writePositive(WriteBuffer out, long value) {
        writeUnsigned(out, value);
    }

    public static int positiveLength(long value) {
        return unsignedNumBlocks(value);
    }

    /* ##################################
      Read and write arbitrary longs
    ################################## */

    private static long convert2Unsigned(long value) {
        return Math.abs(value) << 1 | (value < 0 ? 1 : 0);
    }

    private static long convertFromUnsigned(long value) {
        return ((value & 1) == 1) ? -(value >>> 1) : value >>> 1;
    }

    public static int length(long value) {
        return unsignedNumBlocks(convert2Unsigned(value));
    }

    public static void write(WriteBuffer out, long value) {
        writeUnsigned(out, convert2Unsigned(value));
    }

    public static long read(ScanBuffer in) {
        return convertFromUnsigned(readUnsigned(in));
    }


    /* ##################################
      Read and write positive longs with a specified binary prefix of fixed length
    ################################## */

    public static void writePositiveWithPrefix(WriteBuffer out, long value, long prefix, int prefixBitLen) {
        //Write first byte
        int deltaLen = 8 - prefixBitLen;
        byte first = (byte) (prefix << deltaLen);
        int valueLen = unsignedBitLength(value);
        int mod = valueLen % 7;
        if (mod <= (deltaLen - 1)) {
            int offset = (valueLen - mod);
            first = (byte) (first | (value >>> offset));
            value = value & ((1L << offset) - 1);
            valueLen -= mod;
        } else {
            valueLen += (7 - mod);
        }
        if (valueLen > 0) {
            //Add continue mask to indicate reading further
            first = (byte) (first | (1 << (deltaLen - 1)));
        }
        out.putByte(first);
        if (valueLen > 0) {
            //Keep writing
            writeUnsigned(out, valueLen, value);
        }
    }

    public static int positiveWithPrefixLength(long value, int prefixBitLen) {
        return numVariableBlocks(unsignedBitLength(value) + prefixBitLen);
    }

    public static long[] readPositiveWithPrefix(ReadBuffer in, int prefixBitLen) {
        int first = unsignedByte(in.getByte());
        int deltaLen = 8 - prefixBitLen;
        long prefix = first >> deltaLen;
        long value = first & ((1 << (deltaLen - 1)) - 1);
        if (((first >>> (deltaLen - 1)) & 1) == 1) { //Continue mask
            int deltaPos = in.getPosition();
            long remainder = readUnsigned(in);
            deltaPos = in.getPosition() - deltaPos;
            value = (value << (deltaPos * 7)) + remainder;
        }
        return new long[]{value, prefix};
    }


    /* ##################################
      Write positive longs so that they can be read backwards
      Use positiveLength() for length
    ################################## */

    public static void writePositiveBackward(WriteBuffer out, long value) {
        writeUnsignedBackward(out, value);
    }

    public static int positiveBackwardLength(long value) {
        return unsignedBackwardLength(value);
    }

    public static long readPositiveBackward(ReadBuffer in) {
        return readUnsignedBackward(in);
    }

    /* ##################################
      Write arbitrary longs so that they can be read backwards
      Use length() for length
    ################################## */

    public static void writeBackward(WriteBuffer out, long value) {
        writeUnsignedBackward(out, convert2Unsigned(value));
    }

    public static int backwardLength(long value) {
        return unsignedBackwardLength(convert2Unsigned(value));
    }

    public static long readBackward(ReadBuffer in) {
        return convertFromUnsigned(readUnsignedBackward(in));
    }

    /**
     * The format used is this:
     * - The first bit indicates whether this is the first block (reading backwards, this would be the stop criterion)
     * - In the first byte, the 3 bits after the first bit indicate the number of bytes written minus 3 (since 3 is
     * the minimum number of bytes written. So, if the 3 bits are 010 = 2 => 5 bytes written. The value is aligned to
     * the left to ensure that this encoding is byte order preserving.
     */
    private static void writeUnsignedBackward(WriteBuffer out, long value) {
        int numBytes = unsignedBackwardLength(value);
        int prefixLen = numBytes - 3;
        //Prepare first byte
        byte b = (byte) ((prefixLen << 4) | 0x80); //stop marker (first bit) and length
        for (int i = numBytes - 1; i >= 0; i--) {
            b = (byte) (b | (0x7F & (value >>> (i * 7))));
            out.putByte(b);
            b = 0;
        }
    }

    private static int unsignedBackwardLength(long value) {
        int bitLength = unsignedBitLength(value);
        return Math.max(3, 1 + (bitLength <= 4 ? 0 : (1 + (bitLength - 5) / 7)));
    }

    private static long readUnsignedBackward(ReadBuffer in) {
        int position = in.getPosition();
        int numBytes = 0;
        long value = 0;
        long b;
        while (true) {
            position--;
            b = in.getByte(position);
            if (b < 0) { //First byte
                value = value | ((b & 0x0F) << (7 * numBytes));
                break;
            }
            value = value | (b << (7 * numBytes));
            numBytes++;
        }
        in.movePositionTo(position);
        return value;
    }


}
