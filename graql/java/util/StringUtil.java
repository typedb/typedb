/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package graql.util;

import graql.grammar.GraqlLexer;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class StringUtil {
    private static final Set<String> GRAQL_KEYWORDS = getKeywords();
    // TODO: This is critical knowledge that is lost in a Util class.
    //       Move it to the Query class along with other Syntax rules
    //       once they are moved to //graql package
    private static final Set<String> ALLOWED_ID_KEYWORDS = new HashSet<>(
            Arrays.asList("min", "max", "median", "mean", "std", "sum", "count", "path", "cluster", "degrees", "members", "persist")
    );

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
    public static String quoteString(String string) {
        return "\"" + escapeString(string) + "\"";
    }

    public static String escapeLabelOrId(String value) {
        // TODO: This regex should be saved in Query class once it is moved to //graql package
        if (value.matches("^@?[a-zA-Z_][a-zA-Z0-9_-]*$") &&
                (!GRAQL_KEYWORDS.contains(value)) || ALLOWED_ID_KEYWORDS.contains(value)) {
            return value;
        } else {
            return quoteString(value);
        }
    }

    /**
     * @return all Graql keywords
     */
    private static Set<String> getKeywords() {
        HashSet<String> keywords = new HashSet<>();

        for (int i = 1; i <= GraqlLexer.VOCABULARY.getMaxTokenType(); i ++) {
            if (GraqlLexer.VOCABULARY.getLiteralName(i) != null) {
                String name = GraqlLexer.VOCABULARY.getLiteralName(i);
                keywords.add(name.replaceAll("'", ""));
            }
        }

        return Collections.unmodifiableSet(keywords);
    }

    /**
     * @param value a value in the graph
     * @return the string representation of the value (using quotes if it is already a string)
     */
    public static String valueToString(Object value) {
        if (value instanceof String) {
            return quoteString((String) value);
        } else if (value instanceof Double) {
            DecimalFormat df = new DecimalFormat("#", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
            df.setMinimumFractionDigits(1);
            df.setMaximumFractionDigits(12);
            df.setMinimumIntegerDigits(1);
            return df.format(value);
        } else {
            return value.toString();
        }
    }
}
