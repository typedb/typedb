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

package ai.grakn.graql.internal.parser;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.antlr.GraqlLexer;
import ai.grakn.graql.internal.antlr.GraqlParser;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Class for parsing query strings into valid queries
 */
public class QueryParser {

    private final QueryBuilder queryBuilder;
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     */
    private QueryParser(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        registerDefaultAggregates();
    }

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(QueryBuilder queryBuilder) {
        return new QueryParser(queryBuilder);
    }

    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    public <T extends Query<?>> T parseQuery(String queryString) {
        // We can't be sure the returned query type is correct - even at runtime(!) because Java erases generics.
        //
        // e.g.
        // >> AggregateQuery<Boolean> q = qp.parseQuery("match $x isa movie; aggregate count;");
        // The above will work at compile time AND runtime - it will only fail when the query is executed:
        // >> Boolean bool = q.execute();
        // java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Boolean
        //
        //noinspection unchecked
        return (T) parseQueryFragment(GraqlParser::queryEOF, QueryVisitor::visitQueryEOF, queryString);
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    public List<Pattern> parsePatterns(String patternsString) {
        return parseQueryFragment(GraqlParser::patterns, QueryVisitor::visitPatterns, patternsString);
    }

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    public Pattern parsePattern(String patternString){
        return parseQueryFragment(GraqlParser::pattern, QueryVisitor::visitPattern, patternString);
    }

    public Stream<Object> parseBatchLoad(InputStream inputStream) {
        GraqlLexer lexer = new GraqlLexer(new UnbufferedCharStream(inputStream));
        lexer.setTokenFactory(new CommonTokenFactory(true));
        UnbufferedTokenStream tokens = new UnbufferedTokenStream(lexer);

        // Create an iterable that will keep parsing until EOF
        Iterable<Object> iterable = () -> new Iterator<Object>() {

            private Object pattern = null;

            private Optional<Object> getNext() {

                if (pattern == null) {
                    if (tokens.get(tokens.index()).getType() == Token.EOF) {
                        return Optional.empty();
                    }

                    pattern = parseQueryFragment(GraqlParser::batchPattern, QueryVisitor::visitBatchPattern, tokens);
                }
                return Optional.of(pattern);
            }

            @Override
            public boolean hasNext() {
                return getNext().isPresent();
            }

            @Override
            public Object next() {
                Optional<Object> result = getNext();
                pattern = null;
                return result.orElseThrow(NoSuchElementException::new);
            }
        };

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * @param inputStream a stream representing a list of patterns
     * @return a stream of patterns
     */
    public Stream<Pattern> parsePatterns(InputStream inputStream) {
        GraqlLexer lexer = new GraqlLexer(new UnbufferedCharStream(inputStream));
        lexer.setTokenFactory(new CommonTokenFactory(true));
        UnbufferedTokenStream tokens = new UnbufferedTokenStream(lexer);

        // First parse initial 'insert'
        parseQueryFragment(GraqlParser::insert, QueryVisitor::visitInsert, tokens);

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
        registerAggregate("count", args -> Graql.count());
        registerAggregate("sum", args -> Graql.sum((String) args.get(0)));
        registerAggregate("max", args -> Graql.max((String) args.get(0)));
        registerAggregate("min", args -> Graql.min((String) args.get(0)));
        registerAggregate("average", args -> Graql.average((String) args.get(0)));
        registerAggregate("median", args -> Graql.median((String) args.get(0)));

        registerAggregate("group", args -> {
            if (args.size() < 2) {
                return Graql.group((String) args.get(0));
            } else {
                return Graql.group((String) args.get(0), (Aggregate) args.get(1));
            }
        });
    }
}
