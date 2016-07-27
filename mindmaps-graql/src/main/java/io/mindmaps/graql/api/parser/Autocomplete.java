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

package io.mindmaps.graql.api.parser;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.internal.parser.GraqlLexer;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Token;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An autocomplete result suggesting keywords, types and variables that the user may wish to type
 */
public class Autocomplete {

    private final Set<String> candidates;
    private final int cursorPosition;

    /**
     * @param transaction the transaction to find types in
     * @param query the query to autocomplete
     * @param cursorPosition the cursor position in the query
     * @return an autocomplete object containing potential candidates and cursor position to autocomplete from
     */
    public static Autocomplete create(MindmapsTransaction transaction, String query, int cursorPosition) {
        return new Autocomplete(transaction, query, cursorPosition);
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
     * @param transaction the transaction to find types in
     * @param query the query to autocomplete
     * @param cursorPosition the cursor position in the query
     */
    private Autocomplete(MindmapsTransaction transaction, String query, int cursorPosition) {
        Optional<? extends Token> optToken = getCursorToken(query, cursorPosition);
        candidates = findCandidates(transaction, query, optToken);
        this.cursorPosition = findCursorPosition(cursorPosition, optToken);
    }

    /**
     * @param transaction the transaction to find types in
     * @param query a graql query
     * @param optToken the token the cursor is on in the query
     * @return a set of potential autocomplete words
     */
    private static Set<String> findCandidates(MindmapsTransaction transaction, String query, Optional<? extends Token> optToken) {
        Set<String> allCandidates = Stream.of(getKeywords(), getTypes(transaction), getVariables(query))
                .flatMap(Function.identity()).collect(Collectors.toSet());

        return optToken.map(
                token -> {
                    Set<String> candidates = allCandidates.stream()
                            .filter(candidate -> candidate.startsWith(token.getText()))
                            .collect(Collectors.toSet());

                    if (candidates.size() == 1 && candidates.iterator().next().equals(token.getText())) {
                        return Sets.newHashSet(" ");
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

    /**
     * @param transaction the transaction to find types in
     * @return all type IDs in the ontology
     */
    private static Stream<String> getTypes(MindmapsTransaction transaction) {
        return transaction.getMetaType().instances().stream().map(Concept::getId);
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
