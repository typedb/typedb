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

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.AskQuery;
import io.mindmaps.graql.api.query.DeleteQuery;
import io.mindmaps.graql.api.query.InsertQuery;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.internal.parser.GraqlLexer;
import io.mindmaps.graql.internal.parser.GraqlParser;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import io.mindmaps.graql.internal.parser.QueryVisitor;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.InputStream;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Class for parsing query strings into valid queries
 */
public class QueryParser {

    private final MindmapsTransaction transaction;

    /**
     * Create a query parser with the specified graph
     *  @param transaction  the transaction to operate the query on
     */
    private QueryParser(MindmapsTransaction transaction) {
        this.transaction = transaction;
    }

    /**
     *  @return a query parser with no graph specified
     */
    public static QueryParser create() {
        return new QueryParser(null);
    }

    /**
     * Create a query parser with the specified graph
     *  @param transaction  the transaction to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(MindmapsTransaction transaction) {
        return new QueryParser(transaction);
    }

    /**
     * @param queryString a string representing a match query
     * @return the parsed match query
     */
    public MatchQueryPrinter parseMatchQuery(String queryString) {
        return parseQueryFragment(GraqlParser::matchEOF, QueryVisitor::visitMatchEOF, queryString);
    }

    /**
     * @param queryString a string representing an ask query
     * @return a parsed ask query
     */
    public AskQuery parseAskQuery(String queryString) {
        return parseQueryFragment(GraqlParser::askEOF, QueryVisitor::visitAskEOF, queryString);
    }

    /**
     * @param queryString a string representing an insert query
     * @return a parsed insert query
     */
    public InsertQuery parseInsertQuery(String queryString) {
        return parseQueryFragment(GraqlParser::insertEOF, QueryVisitor::visitInsertEOF, queryString);
    }

    /**
     * @param queryString a string representing a delete query
     * @return a parsed delete query
     */
    public DeleteQuery parseDeleteQuery(String queryString) {
        return parseQueryFragment(GraqlParser::deleteEOF, QueryVisitor::visitDeleteEOF, queryString);
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    public Object parseQuery(String queryString) {
        return parseQueryFragment(GraqlParser::queryEOF, QueryVisitor::visitQueryEOF, queryString);
    }

    /**
     * @param queryString a string representing a list of patterns
     * @return a list of patterns
     */
    public List<Pattern> parsePatterns(String queryString) {
        return parseQueryFragment(GraqlParser::patterns, QueryVisitor::visitPatterns, queryString);
    }

    public Stream<Pattern> parsePatternsStream(InputStream inputStream) {
        GraqlLexer lexer = new GraqlLexer(new UnbufferedCharStream(inputStream));
        lexer.setTokenFactory(new CommonTokenFactory(true));
        UnbufferedTokenStream tokens = new UnbufferedTokenStream(lexer);
        return Stream.generate(
                () -> parseQueryFragment(GraqlParser::patternSep, QueryVisitor::visitPatternSep, tokens)
        );
    }

    /**
     * Parse any part of a Graql query
     * @param parseRule a method on GraqlParser that yields the parse rule you want to use (e.g. GraqlParser::variable)
     * @param visit a method on QueryVisitor that visits the parse rule you specified (e.g. QueryVisitor::visitVariable)
     * @param queryString the string to parse
     * @param <T> The type the query is expected to parse to
     * @param <S> The type of the parse rule being used
     * @return the parsed result
     */
    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit, String queryString
    ) {
        GraqlLexer lexer = getLexer(queryString);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        return parseQueryFragment(parseRule, visit, tokens);
    }

    /**
     * Parse any part of a Graql query
     * @param parseRule a method on GraqlParser that yields the parse rule you want to use (e.g. GraqlParser::variable)
     * @param visit a method on QueryVisitor that visits the parse rule you specified (e.g. QueryVisitor::visitVariable)
     * @param tokens the token stream to read
     * @param <T> The type the query is expected to parse to
     * @param <S> The type of the parse rule being used
     * @return the parsed result
     */
    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit, TokenStream tokens
    ) {
        GraqlParser parser = new GraqlParser(tokens);
        S tree = parseRule.apply(parser);

        if (parser.getNumberOfSyntaxErrors() != 0) {
            throw new IllegalArgumentException(ErrorMessage.SYNTAX_ERROR.getMessage());
        }

        return visit.apply(new QueryVisitor(transaction), tree);
    }

    private GraqlLexer getLexer(String queryString) {
        ANTLRInputStream input = new ANTLRInputStream(queryString);
        return new GraqlLexer(input);
    }
}
