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
import io.mindmaps.graql.internal.parser.GraqlLexer;
import io.mindmaps.graql.internal.parser.GraqlParser;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import io.mindmaps.graql.internal.parser.QueryVisitor;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.function.Function;

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
        return (MatchQueryPrinter) parseQuery(GraqlParser::matchEOF, queryString);
    }

    /**
     * @param queryString a string representing an ask query
     * @return a parsed ask query
     */
    public AskQuery parseAskQuery(String queryString) {
        return (AskQuery) parseQuery(GraqlParser::askEOF, queryString);
    }

    /**
     * @param queryString a string representing an insert query
     * @return a parsed insert query
     */
    public InsertQuery parseInsertQuery(String queryString) {
        return (InsertQuery) parseQuery(GraqlParser::insertEOF, queryString);
    }

    /**
     * @param queryString a string representing a delete query
     * @return a parsed delete query
     */
    public DeleteQuery parseDeleteQuery(String queryString) {
        return (DeleteQuery) parseQuery(GraqlParser::deleteEOF, queryString);
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    public Object parseQuery(String queryString) {
        return parseQuery(GraqlParser::queryEOF, queryString);
    }

    private Object parseQuery(Function<GraqlParser, ParseTree> parseRule, String queryString) {
        GraqlLexer lexer = getLexer(queryString);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        GraqlParser parser = new GraqlParser(tokens);
        ParseTree tree = parseRule.apply(parser);

        BufferedTokenStream allTokens = new BufferedTokenStream(getLexer(queryString));
        allTokens.getTokens();

        if (parser.getNumberOfSyntaxErrors() != 0) {
            throw new IllegalArgumentException(ErrorMessage.SYNTAX_ERROR.getMessage());
        }

        return new QueryVisitor(transaction).visit(tree);
    }

    private GraqlLexer getLexer(String queryString) {
        ANTLRInputStream input = new ANTLRInputStream(queryString);
        return new GraqlLexer(input);
    }
}
