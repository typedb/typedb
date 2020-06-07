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

package grakn.core.graph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class Token {

    public static final char SEPARATOR_CHAR = 0x1e;

    public static final String systemETprefix = Graph.Hidden.hide("T$");

    public static String getSeparatedName(String... components) {
        for (String component : components) verifyName(component);
        return StringUtils.join(components, SEPARATOR_CHAR);
    }

    public static void verifyName(String name) {
        Preconditions.checkArgument(name.indexOf(Token.SEPARATOR_CHAR) < 0,
                "Name can not contains reserved character %s: %s", Token.SEPARATOR_CHAR, name);
    }

    public static String[] splitSeparatedName(String name) {
        return name.split(SEPARATOR_CHAR + "");
    }

    public static final String INTERNAL_INDEX_NAME = "internalindex";

    public static boolean isSystemName(String name) {
        return Graph.Hidden.isHidden(name);
    }

    public static String makeSystemName(String name) {
        return Graph.Hidden.hide(name);
    }


}
