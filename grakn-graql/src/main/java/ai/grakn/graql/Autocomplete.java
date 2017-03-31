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

package ai.grakn.graql;

import ai.grakn.graql.internal.antlr.GraqlLexer;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static ai.grakn.graql.internal.util.StringConverter.GRAQL_KEYWORDS;

/**
 * An autocomplete result suggesting keywords, types and variables that the user may wish to type
 *
 * @author Felix Chapman
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
    public static Autocomplete create(Set<String> types, String query, int cursorPosition) {
        return new Autocomplete(types, query, cursorPosition);
    }

    /**
     * @return all candidate autocomplete words
     */
    public Set<String> getCandidates() {
        return candidates;
    }

    /**
     * @return the cursor position that autocompletions should start from
     */
    public int getCursorPosition() {
        return cursorPosition;
    }

    /**
     * @param types a set of type IDs to autocomplete
     * @param query the query to autocomplete
     * @param cursorPosition the cursor position in the query
     */
    private Autocomplete(Set<String> types, String query, int cursorPosition) {
        Optional<? extends Token> optToken = getCursorToken(query, cursorPosition);
        candidates = findCandidates(types, query, optToken);
        this.cursorPosition = findCursorPosition(cursorPosition, optToken);
    }

    /**
     * @param types the graph to find types in
     * @param query a graql query
     * @param optToken the token the cursor is on in the query
     * @return a set of potential autocomplete words
     */
    private static ImmutableSet<String> findCandidates(Set<String> types, String query, Optional<? extends Token> optToken) {
        ImmutableSet<String> allCandidates = Stream.of(GRAQL_KEYWORDS.stream(), types.stream(), getVariables(query))
                .flatMap(Function.identity()).collect(toImmutableSet());

        return optToken.map(
                token -> {
                    ImmutableSet<String> candidates = allCandidates.stream()
                            .filter(candidate -> candidate.startsWith(token.getText()))
                            .collect(toImmutableSet());

                    if (candidates.size() == 1 && candidates.iterator().next().equals(token.getText())) {
                        return ImmutableSet.of(" ");
                    } else {
                        return candidates;
                    }
                }
        ).orElse(allCandidates);
    }

    /**
     * @param cursorPosition the current cursor position
     * @param optToken the token the cursor is on in the query
     * @return the new cursor position to start autocompleting from
     */
    private int findCursorPosition(int cursorPosition, Optional<? extends Token> optToken) {
        return optToken
                .filter(token -> !candidates.contains(" "))
                .map(Token::getStartIndex)
                .orElse(cursorPosition);
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
    private static Optional<? extends Token> getCursorToken(String query, int cursorPosition) {
        if (query == null) return Optional.empty();

        return getTokens(query).stream()
                .filter(t -> t.getChannel() != Token.HIDDEN_CHANNEL)
                .filter(t -> t.getStartIndex() <= cursorPosition && t.getStopIndex() >= cursorPosition - 1)
                .findFirst();
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
