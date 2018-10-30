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

package ai.grakn.graql;

import ai.grakn.graql.grammar.GraqlLexer;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Token;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.StringConverter.GRAQL_KEYWORDS;
import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * An autocomplete result suggesting keywords, types and variables that the user may wish to type
 *
 * @author Grakn Warriors
 */
public class Autocomplete {

    private final ImmutableSet<String> candidates;
    private final int cursorPosition;

    /**
     * @param types a set of type IDs to autocomplete
     * @param query the query to autocomplete
     * @param cursorPosition the cursor position in the query
     * @return an autocomplete object containing potential candidates and cursor position to autocomplete from
     */
    @CheckReturnValue
    public static Autocomplete create(Set<String> types, String query, int cursorPosition) {
        return new Autocomplete(types, query, cursorPosition);
    }

    /**
     * @return all candidate autocomplete words
     */
    @CheckReturnValue
    public Set<String> getCandidates() {
        return candidates;
    }

    /**
     * @return the cursor position that autocompletions should start from
     */
    @CheckReturnValue
    public int getCursorPosition() {
        return cursorPosition;
    }

    /**
     * @param types a set of type IDs to autocomplete
     * @param query the query to autocomplete
     * @param cursorPosition the cursor position in the query
     */
    private Autocomplete(Set<String> types, String query, int cursorPosition) {
        Token token = getCursorToken(query, cursorPosition);
        candidates = findCandidates(types, query, token);
        this.cursorPosition = findCursorPosition(cursorPosition, token);
    }

    /**
     * @param types the graph to find types in
     * @param query a graql query
     * @param token the token the cursor is on in the query
     * @return a set of potential autocomplete words
     */
    private static ImmutableSet<String> findCandidates(Set<String> types, String query, Token token) {
        ImmutableSet<String> allCandidates = Stream.of(GRAQL_KEYWORDS.stream(), types.stream(), getVariables(query))
                .flatMap(Function.identity()).collect(toImmutableSet());

        if (token != null) {
            ImmutableSet<String> candidates = allCandidates.stream()
                    .filter(candidate -> candidate.startsWith(token.getText()))
                    .collect(toImmutableSet());

            if (candidates.size() == 1 && candidates.iterator().next().equals(token.getText())) {
                return ImmutableSet.of(" ");
            } else {
                return candidates;
            }
        } else {
            return allCandidates;
        }
    }

    /**
     * @param cursorPosition the current cursor position
     * @param token the token the cursor is on in the query
     * @return the new cursor position to start autocompleting from
     */
    private int findCursorPosition(int cursorPosition, Token token) {
        if (!candidates.contains(" ") && token != null) return token.getStartIndex();
        else return cursorPosition;
    }

    /**
     * @param query a graql query
     * @return all variable names occurring in the query
     */
    private static Stream<String> getVariables(String query) {
        List<? extends Token> allTokens = getTokens(query);

        if (allTokens.size() > 0) allTokens.remove(allTokens.size() - 1);
        return allTokens.stream()
                .filter(t -> t.getType() == GraqlLexer.VARIABLE)
                .map(Token::getText);
    }

    /**
     * @param query a graql query
     * @param cursorPosition the cursor position in the query
     * @return the token at the cursor position in the given graql query
     */
    @Nullable
    private static Token getCursorToken(String query, int cursorPosition) {
        if (query == null) return null;

        return getTokens(query).stream()
                .filter(t -> t.getChannel() != Token.HIDDEN_CHANNEL)
                .filter(t -> t.getStartIndex() <= cursorPosition && t.getStopIndex() >= cursorPosition - 1)
                .findFirst().orElse(null);
    }

    /**
     * @param query a graql query
     * @return a list of tokens from running the lexer on the query
     */
    private static List<? extends Token> getTokens(String query) {
        ANTLRInputStream input = new ANTLRInputStream(query);
        GraqlLexer lexer = new GraqlLexer(input);

        // Ignore syntax errors
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener());

        return lexer.getAllTokens();
    }
}
