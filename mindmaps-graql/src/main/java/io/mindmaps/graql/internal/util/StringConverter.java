/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.util;

import org.apache.commons.lang.StringEscapeUtils;

/**
 * Class for converting Graql strings, used in the parser and for toString methods
 */
public class StringConverter {

    private StringConverter() {}

    /**
     * @param string the string to unescape
     * @return the unescaped string, replacing any backslash escapes with the real characters
     */
    public static String unescapeString(String string) {
        return StringEscapeUtils.unescapeJavaScript(string);
    }

    /**
     * @param string the string to escape
     * @return the escaped string, replacing any escapable characters with backslashes
     */
    public static String escapeString(String string) {
        return StringEscapeUtils.escapeJavaScript(string);
    }

    /**
     * @param string a string to quote and escape
     * @return a string, surrounded with double quotes and escaped
     */
    private static String quoteString(String string) {
        return "\"" + escapeString(string) + "\"";
    }

    /**
     * @param value a value in the graph
     * @return the string representation of the value (using quotes if it is already a string)
     */
    public static String valueToString(Object value) {
        if (value instanceof String) {
            return quoteString((String) value);
        } else {
            return value.toString();
        }
    }

    /**
     * @param id an id of a concept
     * @return
     * The id of the concept correctly escaped in graql.
     * If the ID doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String idToString(String id) {
        if (id.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$")) {
            return id;
        } else {
            return quoteString(id);
        }
    }
}
