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

package ai.grakn.graql.internal.util;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.grammar.GraqlLexer;
import ai.grakn.util.StringUtil;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

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
     * @param id an ID of a concept
     * @return
     * The id of the concept correctly escaped in graql.
     * If the ID doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String idToString(ConceptId id) {
        return escapeLabelOrId(id.getValue());
    }

    @Nullable
    public static String nullableIdToString(@Nullable ConceptId id) {
        return id != null ? escapeLabelOrId(id.getValue()) : null;
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

    private static String escapeLabelOrId(String value) {
        if (value.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$") && !GRAQL_KEYWORDS.contains(value)) {
            return value;
        } else {
            return StringUtil.quoteString(value);
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
