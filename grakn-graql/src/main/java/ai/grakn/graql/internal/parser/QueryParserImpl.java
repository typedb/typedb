/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.antlr.GraqlLexer;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.antlr.GraqlParser.PatternContext;
import ai.grakn.graql.internal.antlr.GraqlParser.PatternsContext;
import ai.grakn.graql.internal.antlr.GraqlParser.QueryContext;
import ai.grakn.graql.internal.antlr.GraqlParser.QueryEOFContext;
import ai.grakn.graql.internal.antlr.GraqlParser.QueryListContext;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import ai.grakn.graql.internal.template.TemplateParser;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Default implementation of {@link QueryParser}
 *
 * @author Felix Chapman
 */
public class QueryParserImpl implements QueryParser {

    private final QueryBuilder queryBuilder;
    private final TemplateParser templateParser = TemplateParser.create();
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();
    private boolean defineAllVars = false;

    public static final ImmutableBiMap<String, AttributeType.DataType<?>> DATA_TYPES = ImmutableBiMap.of(
            "long", AttributeType.DataType.LONG,
            "double", AttributeType.DataType.DOUBLE,
            "string", AttributeType.DataType.STRING,
            "boolean", AttributeType.DataType.BOOLEAN,
            "date", AttributeType.DataType.DATE
    );

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     */
    private QueryParserImpl(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(QueryBuilder queryBuilder) {
        QueryParserImpl parser = new QueryParserImpl(queryBuilder);
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

    @Override
    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
    }

    @Override
    public void defineAllVars(boolean defineAllVars) {
        this.defineAllVars = defineAllVars;
    }

    @Override
    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of QUERY.
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
        return (T) QUERY_EOF.parse(queryString);
    }

    /**
     * @param reader a reader representing several queries
     * @return a list of queries
     */
    @Override
    public <T extends Query<?>> Stream<T> parseList(Reader reader) {
        UnbufferedCharStream charStream = new UnbufferedCharStream(reader);
        GraqlErrorListener errorListener = GraqlErrorListener.withoutQueryString();
        GraqlLexer lexer = createLexer(charStream, errorListener);

        /*
            We tell the lexer to copy the text into each generated token.
            Normally when calling `Token#getText`, it will look into the underlying `TokenStream` and call
            `TokenStream#size` to check it is in-bounds. However, `UnbufferedTokenStream#size` is not supported
            (because then it would have to read the entire input). To avoid this issue, we set this flag which will
            copy over the text into each `Token`, s.t. that `Token#getText` will just look up the copied text field.
        */
        lexer.setTokenFactory(new CommonTokenFactory(true));

        // Use an unbuffered token stream so we can handle extremely large input strings
        UnbufferedTokenStream tokenStream = new UnbufferedTokenStream(ChannelTokenSource.of(lexer));

        GraqlParser parser = createParser(tokenStream, errorListener);

        /*
            The "bail" error strategy prevents us reading all the way to the end of the input, e.g.

            ```
            match $x isa person; insert $x has name "Bob"; match $x isa movie; get;
                                                           ^
            ```

            In this example, when ANTLR reaches the indicated `match`, it considers two possibilities:

            1. this is the end of the query
            2. the user has made a mistake. Maybe they accidentally pasted the `match` here.

            Because of case 2, ANTLR will parse beyond the `match` in order to produce a more helpful error message.
            This causes memory issues for very large queries, so we use the simpler "bail" strategy that will
            immediately stop when it hits `match`.
        */
        parser.setErrorHandler(new BailErrorStrategy());

        // This is a lazy iterator that will only consume a single query at a time, without parsing any further.
        // This means it can pass arbitrarily long streams of queries in constant memory!
        Iterable<T> queryIterator = () -> new AbstractIterator<T>() {
            @Nullable
            @Override
            protected T computeNext() {
                int latestToken = tokenStream.LA(1);
                if (latestToken == Token.EOF) {
                    endOfData();
                    return null;
                } else {
                    // This will parse and consume a single query, even if it doesn't reach an EOF
                    // When we next run it, it will start where it left off in the stream
                    return (T) QUERY.parse(parser, errorListener);
                }
            }
        };

        return StreamSupport.stream(queryIterator.spliterator(), false);
    }

    @Override
    public <T extends Query<?>> Stream<T> parseList(String queryString) {
        return (Stream<T>) QUERY_LIST.parse(queryString);
    }

    @Override
    public List<Pattern> parsePatterns(String patternsString) {
        return PATTERNS.parse(patternsString);
    }

    @Override
    public Pattern parsePattern(String patternString){
        return PATTERN.parse(patternString);
    }

    @Override
    public <T extends Query<?>> Stream<T> parseTemplate(String template, Map<String, Object> data) {
        return parseList(templateParser.parseTemplate(template, data));
    }
    private static GraqlLexer createLexer(CharStream input, GraqlErrorListener errorListener) {
        GraqlLexer lexer = new GraqlLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        return lexer;
    }

    private static GraqlParser createParser(TokenStream tokens, GraqlErrorListener errorListener) {
        GraqlParser parser = new GraqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        return parser;
    }

    private GraqlConstructor getQueryVisitor() {
        ImmutableMap<String, Function<List<Object>, Aggregate>> immutableAggregates =
                ImmutableMap.copyOf(aggregateMethods);

        return new GraqlConstructor(immutableAggregates, queryBuilder, defineAllVars);
    }

    // Aggregate methods that include other aggregates, such as group are not necessarily safe at runtime.
    // This is unavoidable in the parser.
    @SuppressWarnings("unchecked")
    private void registerDefaultAggregates() {
        registerAggregate("count", 0, args -> Graql.count());
        registerAggregate("ask", 0, args -> Graql.ask());
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

    private final QueryPart<QueryListContext, Stream<? extends Query<?>>> QUERY_LIST =
            createQueryPart(parser -> parser.queryList(),
                    (constructor, context) -> constructor.visitQueryList(context));

    private final QueryPart<QueryEOFContext, Query<?>> QUERY_EOF =
            createQueryPart(parser -> parser.queryEOF(),
                    (constructor, context) -> constructor.visitQueryEOF(context));

    private final QueryPart<QueryContext, Query<?>> QUERY =
            createQueryPart(parser -> parser.query(),
                    (constructor, context) -> constructor.visitQuery(context));

    private final QueryPart<PatternsContext, List<Pattern>> PATTERNS =
            createQueryPart(parser -> parser.patterns(),
                    (constructor, context) -> constructor.visitPatterns(context));

    private final QueryPart<PatternContext, Pattern> PATTERN =
            createQueryPart(parser -> parser.pattern(),
                    (constructor, context) -> constructor.visitPattern(context));

    private <S extends ParseTree, T> QueryPart<S, T> createQueryPart(
            Function<GraqlParser, S> parseTree, BiFunction<GraqlConstructor, S, T> visit) {

        return new QueryPart<S, T>() {
            @Override
            S parseTree(GraqlParser parser) {
                return parseTree.apply(parser);
            }

            @Override
            T visit(GraqlConstructor visitor, S context) {
                return visit.apply(visitor, context);
            }
        };
    }

    /**
     * Represents a part of a Graql query to parse, such as "pattern".
     */
    private abstract class QueryPart<S extends ParseTree, T> {

        /**
         * Get a {@link ParseTree} from a {@link GraqlParser}.
         */
        abstract S parseTree(GraqlParser parser) throws ParseCancellationException;

        /**
         * Parse the {@link ParseTree} into a Java object using a {@link GraqlConstructor}.
         */
        abstract T visit(GraqlConstructor visitor, S context);

        /**
         * Parse the string into a Java object
         */
        final T parse(String queryString) {
            ANTLRInputStream charStream = new ANTLRInputStream(queryString);
            GraqlErrorListener errorListener = GraqlErrorListener.of(queryString);

            GraqlLexer lexer = createLexer(charStream, errorListener);

            CommonTokenStream tokens = new CommonTokenStream(lexer);
            GraqlParser parser = createParser(tokens, errorListener);

            return parse(parser, errorListener);
        }

        /**
         * Parse the {@link GraqlParser} into a Java object, where errors are reported to the given
         * {@link GraqlErrorListener}.
         */
        final T parse(GraqlParser parser, GraqlErrorListener errorListener) {
            S tree;

            try {
                tree = parseTree(parser);
            } catch (ParseCancellationException e) {
                // If we're using the BailErrorStrategy, we will throw here
                // This strategy is designed for parsing very large files and cannot provide useful error information
                throw GraqlSyntaxException.create("syntax error");
            }

            if (errorListener.hasErrors()) {
                throw GraqlSyntaxException.create(errorListener.toString());
            }

            return visit(getQueryVisitor(), tree);
        }
    }
}
