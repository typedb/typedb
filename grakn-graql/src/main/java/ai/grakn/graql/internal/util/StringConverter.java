/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.util;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.internal.antlr.GraqlLexer;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashSet;
import java.util.Locale;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;

/**
 * Class for converting Graql strings, used in the parser and for toString methods
 *
 * @author Felix Chapman
 */
public class StringConverter {

    private static final ImmutableSet<String> ALLOWED_ID_KEYWORDS = ImmutableSet.of(
            "min", "max", "median", "mean", "std", "sum", "count", "path", "cluster", "degrees", "members", "persist"
    );

    public static final ImmutableSet<String> GRAQL_KEYWORDS = getKeywords().collect(toImmutableSet());

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

    /**
     * @param id an ID of a concept
     * @return
     * The id of the concept correctly escaped in graql.
     * If the ID doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String idToString(ConceptId id) {
        return escapeLabelOrId(id.getValue());
    }

    /**
     * @param typeLabel a label of a type
     * @return
     * The label of the type correctly escaped in graql.
     * If the label doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String typeLabelToString(TypeLabel typeLabel) {
        return escapeLabelOrId(typeLabel.getValue());
    }

    private static String escapeLabelOrId(String value) {
        if (value.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$") && !GRAQL_KEYWORDS.contains(value)) {
            return value;
        } else {
            return quoteString(value);
        }
    }

    /**
     * @return all Graql keywords
     */
    private static Stream<String> getKeywords() {
        HashSet<String> keywords = new HashSet<>();

        for (int i = 1; GraqlLexer.VOCABULARY.getLiteralName(i) != null; i ++) {
            String name = GraqlLexer.VOCABULARY.getLiteralName(i);
            keywords.add(name.replaceAll("'", ""));
        }

        return keywords.stream().filter(keyword -> !ALLOWED_ID_KEYWORDS.contains(keyword));
    }
}
