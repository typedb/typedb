/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

/**
 * Utility class for encoding longs in strings based on:
 * See <a href="https://stackoverflow.com/questions/2938482/encode-decode-a-long-to-a-string-using-a-fixed-set-of-letters-in-java">stackoverflow</a>
 */
public class LongEncoding {

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";

    public static long decode(String s) {
        final int B = BASE_SYMBOLS.length();
        long num = 0;
        for (char ch : s.toCharArray()) {
            num *= B;
            int pos = BASE_SYMBOLS.indexOf(ch);
            if (pos < 0) throw new NumberFormatException("Symbol set does not match string");
            num += pos;
        }
        return num;
    }

    public static String encode(long num) {
        Preconditions.checkArgument(num >= 0, "Expected non-negative number: " + num);
        final int B = BASE_SYMBOLS.length();
        StringBuilder sb = new StringBuilder();
        while (num != 0) {
            sb.append(BASE_SYMBOLS.charAt((int) (num % B)));
            num /= B;
        }
        return sb.reverse().toString();
    }

}
