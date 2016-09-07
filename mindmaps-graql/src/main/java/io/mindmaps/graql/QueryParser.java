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

package io.mindmaps.graql;

import com.google.common.collect.ImmutableMap;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.internal.parser.*;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.InputStream;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.mindmaps.graql.Graql.*;

/**
 * Class for parsing query strings into valid queries
 */
public class QueryParser {

    private final QueryBuilder queryBuilder;
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();

    /**
     * Create a query parser with no specified graph
     */
    private QueryParser() {
        queryBuilder = withoutGraph();
        registerDefaultAggregates();
    }

    /**
     * Create a query parser with the specified graph
     *  @param graph the graph to operate the query on
     */
    private QueryParser(MindmapsGraph graph) {
        queryBuilder = withGraph(graph);
        registerDefaultAggregates();
    }

    /**
     *  @return a query parser with no graph specified
     */
    public static QueryParser create() {
        return new QueryParser();
    }

    /**
     * Create a query parser with the specified graph
     *  @param graph the graph to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(MindmapsGraph graph) {
        return new QueryParser(graph);
    }

    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
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

        // Create an iterable that will keep parsing until EOF
        Iterable<Pattern> iterable = () -> new Iterator<Pattern>() {

            private Pattern pattern = null;

            private Optional<Pattern> getNext() {

                if (pattern == null) {
                    if (tokens.get(tokens.index()).getType() == Token.EOF) {
                        return Optional.empty();
                    }

                    pattern = parseQueryFragment(GraqlParser::patternSep, QueryVisitor::visitPatternSep, tokens);
                }
                return Optional.of(pattern);
            }

            @Override
            public boolean hasNext() {
                return getNext().isPresent();
            }

            @Override
            public Pattern next() {
                Optional<Pattern> result = getNext();
                pattern = null;
                return result.orElseThrow(NoSuchElementException::new);
            }
        };

        return StreamSupport.stream(iterable.spliterator(), false);
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

        GraqlErrorListener errorListener = new GraqlErrorListener(queryString);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        return parseQueryFragment(parseRule, visit, errorListener, tokens);
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
        GraqlErrorListener errorListener = new GraqlErrorListener(tokens.getText());
        return parseQueryFragment(parseRule, visit, errorListener, tokens);
    }

    /**
     * Parse any part of a Graql query
     * @param parseRule a method on GraqlParser that yields the parse rule you want to use (e.g. GraqlParser::variable)
     * @param visit a method on QueryVisitor that visits the parse rule you specified (e.g. QueryVisitor::visitVariable)
     * @param errorListener an object that will listen for errors in the lexer or parser
     * @param tokens the token stream to read
     * @param <T> The type the query is expected to parse to
     * @param <S> The type of the parse rule being used
     * @return the parsed result
     */
    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit,
            GraqlErrorListener errorListener, TokenStream tokens
    ) {
        GraqlParser parser = new GraqlParser(tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        S tree = parseRule.apply(parser);

        if (errorListener.hasErrors()) {
            throw new IllegalArgumentException(errorListener.toString());
        }

        return visit.apply(getQueryVisitor(), tree);
    }

    private GraqlLexer getLexer(String queryString) {
        ANTLRInputStream input = new ANTLRInputStream(queryString);
        return new GraqlLexer(input);
    }

    private QueryVisitor getQueryVisitor() {
        ImmutableMap<String, Function<List<Object>, Aggregate>> immutableAggregates =
                ImmutableMap.copyOf(aggregateMethods);

        return new QueryVisitor(immutableAggregates, queryBuilder);
    }

    private void registerDefaultAggregates() {
        registerAggregate("count", args -> count());
        registerAggregate("sum", args -> sum((String) args.get(0)));
        registerAggregate("max", args -> max((String) args.get(0)));
        registerAggregate("min", args -> min((String) args.get(0)));
        registerAggregate("average", args -> average((String) args.get(0)));
        registerAggregate("median", args -> median((String) args.get(0)));

        registerAggregate("group", args -> {
            if (args.size() < 2) {
                return group((String) args.get(0));
            } else {
                return group((String) args.get(0), (Aggregate) args.get(1));
            }
        });
    }
}
