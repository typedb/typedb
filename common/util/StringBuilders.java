/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.util;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringBuilders {

    public static final String SEMICOLON_NEWLINE_X2 = ";\n\n";
    public static final String COMMA_NEWLINE_INDENT = ",\n" + indent(1);

    public static String indent(int indent) {
        return IntStream.range(0, indent * 4).mapToObj(i -> " ").collect(Collectors.joining());
    }
}
