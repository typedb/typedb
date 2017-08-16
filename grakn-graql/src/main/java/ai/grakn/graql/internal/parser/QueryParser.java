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

import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.antlr.GraqlLexer;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.ListTokenSource;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Class for parsing query strings into valid queries
 *
 * @author Felix Chapman
 */
public class QueryParser {

    private final QueryBuilder queryBuilder;
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();

    public static final ImmutableBiMap<String, ResourceType.DataType> DATA_TYPES = ImmutableBiMap.of(
            "long", ResourceType.DataType.LONG,
            "double", ResourceType.DataType.DOUBLE,
            "string", ResourceType.DataType.STRING,
            "boolean", ResourceType.DataType.BOOLEAN,
            "date", ResourceType.DataType.DATE
    );

    private static final Set<Integer> NEW_QUERY_TOKENS = ImmutableSet.of(GraqlLexer.MATCH, GraqlLexer.INSERT);

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     */
    private QueryParser(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(QueryBuilder queryBuilder) {
        QueryParser parser = new QueryParser(queryBuilder);
        parser.registerDefaultAggregates();
        return parser;
    }

    private void registerAggregate(String name, int numArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        registerAggregate(name, numArgs, numArgs, aggregateMethod);
    }

    private void registerAggregate(
            String name, int minArgs, int maxArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, args -> {
            if (args.size() < minArgs || args.size() > maxArgs) {
                throw GraqlQueryException.incorrectAggregateArgumentNumber(name, minArgs, maxArgs, args);
            }
            return aggregateMethod.apply(args);
        });
    }

    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    @SuppressWarnings("unchecked")
    public <T extends Query<?>> T parseQuery(String queryString) {
        // We can't be sure the returned query type is correct - even at runtime(!) because Java erases generics.
        //
        // e.g.
        // >> AggregateQuery<Boolean> q = qp.parseQuery("match $x isa movie; aggregate count;");
        // The above will work at compile time AND runtime - it will only fail when the query is executed:
        // >> Boolean bool = q.execute();
        // java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Boolean
        return (T) parseQueryFragment(GraqlParser::queryEOF, QueryVisitor::visitQueryEOF, queryString, getLexer(queryString));
    }

    /**
     * @param queryString a string representing several queries
     * @return a list of queries
     */
    public <T extends Query<?>> Stream<T> parseList(String queryString) {
        GraqlLexer lexer = getLexer(queryString);

        GraqlErrorListener errorListener = new GraqlErrorListener(queryString);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        UnbufferedTokenStream tokenStream = new UnbufferedTokenStream(lexer);

        // Merge any match...insert queries together
        // TODO: Find a way to NOT do this horrid thing
        AbstractIterator<T> iterator = new AbstractIterator<T>() {
            @Nullable
            T previous = null;

            @Nullable
            @Override
            protected T computeNext() {
                if (tokenStream.LA(1) == GraqlLexer.EOF) {
                    if (previous != null) {
                        return swapPrevious(null);
                    } else {
                        endOfData();
                        return null;
                    }
                }

                TokenSource oneQuery = consumeOneQuery(tokenStream);
                T current = parseQueryFragment(GraqlParser::query, (q, t) -> (T) q.visitQuery(t), oneQuery, errorListener);

                if (previous == null) {
                    previous = current;
                    return computeNext();
                } else if (previous instanceof MatchQuery && current instanceof InsertQuery) {
                    return (T) joinMatchInsert((MatchQuery) swapPrevious(null), (InsertQuery) current);
                } else {
                    return swapPrevious(current);
                }
            }

            @Nullable
            private T swapPrevious(@Nullable T newPrevious) {
                T oldPrevious = previous;
                previous = newPrevious;
                return oldPrevious;
            }

            private InsertQuery joinMatchInsert(MatchQuery match, InsertQuery insert) {
                return match.insert(insert.admin().varPatterns());
            }
        };

        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    public List<Pattern> parsePatterns(String patternsString) {
        return parseQueryFragment(GraqlParser::patterns, QueryVisitor::visitPatterns, patternsString, getLexer(patternsString));
    }

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    public Pattern parsePattern(String patternString){
        return parseQueryFragment(GraqlParser::pattern, QueryVisitor::visitPattern, patternString, getLexer(patternString));
    }

    /**
     * Parse any part of a Graql query
     * @param parseRule a method on GraqlParser that yields the parse rule you want to use (e.g. GraqlParser::variable)
     * @param visit a method on QueryVisitor that visits the parse rule you specified (e.g. QueryVisitor::visitVariable)
     * @param queryString the string to parse
     * @param lexer
     * @param <T> The type the query is expected to parse to
     * @param <S> The type of the parse rule being used
     * @return the parsed result
     */
    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit, String queryString, GraqlLexer lexer
    ) {
        GraqlErrorListener errorListener = new GraqlErrorListener(queryString);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        return parseQueryFragment(parseRule, visit, lexer, errorListener);
    }

    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit,
            TokenSource source, GraqlErrorListener errorListener
    ) {
        CommonTokenStream tokens = new CommonTokenStream(source);

        GraqlParser parser = new GraqlParser(tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        S tree = parseRule.apply(parser);

        if (errorListener.hasErrors()) {
            throw GraqlSyntaxException.parsingError(errorListener.toString());
        }

        return visit.apply(getQueryVisitor(), tree);
    }

    /**
     * Consume a single query from the given token stream.
     *
     * @param tokenStream the {@link TokenStream} to consume
     * @return a new {@link TokenSource} containing the tokens comprising the query
     */
    private TokenSource consumeOneQuery(TokenStream tokenStream) {
        List<Token> tokens = new ArrayList<>();

        boolean startedQuery = false;

        while (true) {
            Token token = tokenStream.LT(1);
            boolean isNewQuery = NEW_QUERY_TOKENS.contains(token.getType());
            boolean isEndOfTokenStream = token.getType() == IntStream.EOF;
            boolean isEndOfFirstQuery = startedQuery && isNewQuery;

            // Stop parsing tokens after reaching the end of the first query
            if (isEndOfTokenStream || isEndOfFirstQuery) break;

            if (isNewQuery) startedQuery = true;

            tokens.add(token);
            tokenStream.consume();
        }

        return new ListTokenSource(tokens);
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

    // Aggregate methods that include other aggregates, such as group are not necessarily safe at runtime.
    // This is unavoidable in the parser.
    @SuppressWarnings("unchecked")
    private void registerDefaultAggregates() {
        registerAggregate("count", 0, args -> Graql.count());
        registerAggregate("sum", 1, args -> Aggregates.sum((Var) args.get(0)));
        registerAggregate("max", 1, args -> Aggregates.max((Var) args.get(0)));
        registerAggregate("min", 1, args -> Aggregates.min((Var) args.get(0)));
        registerAggregate("mean", 1, args -> Aggregates.mean((Var) args.get(0)));
        registerAggregate("median", 1, args -> Aggregates.median((Var) args.get(0)));
        registerAggregate("std", 1, args -> Aggregates.std((Var) args.get(0)));

        registerAggregate("group", 1, 2, args -> {
            if (args.size() < 2) {
                return Aggregates.group((Var) args.get(0));
            } else {
                return Aggregates.group((Var) args.get(0), (Aggregate) args.get(1));
            }
        });
    }
}
