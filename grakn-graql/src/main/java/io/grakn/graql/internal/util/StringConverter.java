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

package io.grakn.graql.internal.util;

import com.google.common.collect.ImmutableSet;
import io.grakn.concept.Concept;
import io.grakn.concept.Type;
import io.grakn.graql.internal.antlr.GraqlLexer;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.grakn.graql.internal.query.match.MatchQueryInternal.colorKeyword;
import static io.grakn.graql.internal.query.match.MatchQueryInternal.colorType;
import static io.grakn.graql.internal.util.CommonUtil.toImmutableSet;

/**
 * Class for converting Graql strings, used in the parser and for toString methods
 */
public class StringConverter {

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
        if (id.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$") && !GRAQL_KEYWORDS.contains(id)) {
            return id;
        } else {
            return quoteString(id);
        }
    }

    /**
     * Get a Graql string representation of an object, for printing as a result
     */
    public static String graqlString(Object object) {
        StringBuilder sb = new StringBuilder();
        graqlString(sb, false, object);
        return sb.toString();
    }

    private static StringBuilder graqlString(StringBuilder sb, boolean inner, Object object) {
        if (object instanceof Concept) {
            graqlString(sb, (Concept) object);
        } else if (object instanceof Boolean) {
            graqlString(sb, (boolean) object);
        } else if (object instanceof Optional) {
            graqlString(sb, inner, (Optional<?>) object);
        } else if (object instanceof Collection) {
            graqlString(sb, inner, (Collection<?>) object);
        } else if (object instanceof Map) {
            graqlString(sb, inner, (Map<?, ?>) object);
        } else if (object instanceof Map.Entry) {
            graqlString(sb, (Map.Entry<?, ?>) object);
        } else {
            sb.append(object.toString());
        }

        return sb;
    }

    private static void graqlString(StringBuilder sb, Concept concept) {
        // Display values for resources and ids for everything else
        if (concept.isResource()) {
            sb.append(colorKeyword("value "));
            sb.append(StringConverter.valueToString(concept.asResource().getValue()));
        } else {
            sb.append(colorKeyword("id "));
            sb.append("\"").append(StringConverter.escapeString(concept.getId())).append("\"");
        }

        // Display type of each concept
        Type type = concept.type();
        if (type != null) {
            sb.append(colorKeyword(" isa ")).append(colorType(StringConverter.idToString(type.getId())));
        }

        // Display lhs and rhs for rules
        if (concept.isRule()) {
            sb.append(colorKeyword(" lhs ")).append("{ ").append(concept.asRule().getLHS()).append(" }");
            sb.append(colorKeyword(" rhs ")).append("{ ").append(concept.asRule().getRHS()).append(" }");
        }
    }

    private static void graqlString(StringBuilder sb, boolean bool) {
        if (bool) {
            sb.append(ANSI.color("True", ANSI.GREEN));
        } else {
            sb.append(ANSI.color("False", ANSI.RED));
        }
    }

    private static void graqlString(StringBuilder sb, boolean inner, Optional<?> optional) {
        if (optional.isPresent()) {
            graqlString(sb, inner, optional.get());
        } else {
            sb.append("Nothing");
        }
    }

    private static void graqlString(StringBuilder sb, boolean inner, Collection<?> collection) {
        if (inner) {
            sb.append("{");
            collection.stream().findFirst().ifPresent(item -> graqlString(sb, true, item));
            collection.stream().skip(1).forEach(item -> graqlString(sb.append(", "), true, item));
            sb.append("}");
        } else {
            collection.forEach(item -> graqlString(sb, true, item).append("\n"));
        }
    }

    private static void graqlString(StringBuilder sb, boolean inner, Map<?, ?> map) {
        if (!map.entrySet().isEmpty()) {
            Map.Entry<?, ?> entry = map.entrySet().iterator().next();

            // If this looks like a graql result, assume the key is a variable name
            if (entry.getKey() instanceof String && entry.getValue() instanceof Concept) {
                map.forEach((name, concept) ->
                    sb.append("$").append(name).append(" ").append(StringConverter.graqlString(concept)).append("; ")
                );
                return;
            }
        }

        graqlString(sb, inner, map.entrySet());
    }

    private static void graqlString(StringBuilder sb, Map.Entry<?, ?> entry) {
        graqlString(sb, true, entry.getKey()).append(":\t");
        graqlString(sb, true, entry.getValue());
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

        return keywords.stream();
    }
}
