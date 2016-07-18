package io.mindmaps.graql.internal;

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
    private static String escapeString(String string) {
        return StringEscapeUtils.escapeJavaScript(string);
    }

    /**
     * @param value a value in the graph
     * @return the string representation of the value (using quotes if it is already a string)
     */
    public static String valueToString(Object value) {
        if (value instanceof String) {
            return "\"" + escapeString((String) value) + "\"";
        } else {
            return value.toString();
        }
    }
}
