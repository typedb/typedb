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
package grakn.core.graql.util;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import graql.grammar.GraqlLexer;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashSet;
import java.util.Locale;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;

/**
 * Some helper methods in dealing with strings in the context of Graql.
 */
public class StringUtil {

    private static final ImmutableSet<String> ALLOWED_ID_KEYWORDS = ImmutableSet.of(
            "min", "max", "median", "mean", "std", "sum", "count", "path", "cluster", "degrees", "members", "persist"
    );
    public static final ImmutableSet<String> GRAQL_KEYWORDS = getKeywords().collect(toImmutableSet());

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
        return "\"" + StringUtil.escapeString(string) + "\"";
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
     * @param label a label of a type
     * @return
     * The label of the type correctly escaped in graql.
     * If the label doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String typeLabelToString(Label label) {
        return escapeLabelOrId(label.getValue());
    }


    public static String typeLabelToString(String label) {
        return escapeLabelOrId(label);
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
