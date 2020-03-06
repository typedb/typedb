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


package grakn.core.graph.util.encoding;

import com.google.common.base.Preconditions;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class StringEncoding {

    public static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    public static boolean isAsciiString(String input) {
        Preconditions.checkNotNull(input);
        for (int i = 0; i < input.length(); i++) {
            int c = input.charAt(i);
            if (c > 127 || c <= 0) return false;
        }
        return true;
    }

    //Similar to StringSerializer
    public static int writeAsciiString(byte[] array, int startPos, String attribute) {
        Preconditions.checkArgument(isAsciiString(attribute));
        if (attribute.length() == 0) {
            array[startPos++] = (byte) 0x80;
        } else {
            for (int i = 0; i < attribute.length(); i++) {
                int c = attribute.charAt(i);
                byte b = (byte) c;
                if (i + 1 == attribute.length()) b |= 0x80; //End marker
                array[startPos++] = b;
            }
        }
        return startPos;
    }

    public static String readAsciiString(byte[] array, int startPos) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & array[startPos++];
            if (c != 0x80) sb.append((char) (c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        return sb.toString();
    }

    public static int getAsciiByteLength(String attribute) {
        Preconditions.checkArgument(isAsciiString(attribute));
        return attribute.isEmpty() ? 1 : attribute.length();
    }

    public static String launder(String input) {
        Preconditions.checkNotNull(input);
        final StringBuilder sb = new StringBuilder();
        input.chars().forEach(c -> sb.append((char) Integer.valueOf(c).intValue()));
        return sb.toString();
    }

}
