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

package grakn.core.graql.parser;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.grammar.GraqlLexer;
import grakn.core.graql.grammar.GraqlParser;
import grakn.core.graql.grammar.GraqlParser.PatternContext;
import grakn.core.graql.grammar.GraqlParser.PatternsContext;
import grakn.core.graql.grammar.GraqlParser.QueryContext;
import grakn.core.graql.grammar.GraqlParser.QueryEOFContext;
import grakn.core.graql.grammar.GraqlParser.QueryListContext;
import grakn.core.graql.internal.template.TemplateParser;
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.Pattern;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.Var;
import grakn.core.graql.query.aggregate.MaxAggregate;
import grakn.core.graql.query.aggregate.MeanAggregate;
import grakn.core.graql.query.aggregate.MedianAggregate;
import grakn.core.graql.query.aggregate.MinAggregate;
import grakn.core.graql.query.aggregate.StdAggregate;
import grakn.core.graql.query.aggregate.SumAggregate;
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
 * Class for parsing query strings into valid queries
 */
public class QueryParser {

    private final QueryBuilder queryBuilder;
    private final TemplateParser templateParser = TemplateParser.create();
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();
    private boolean defineAllVars = false;

    /**
     * Create a query parser with the specified graph
     *
     * @param queryBuilder the QueryBuilderImpl to operate the query on
     */
    private QueryParser(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Create a query parser with the specified graph
     *
     * @param queryBuilder the QueryBuilderImpl to operate the query on
     * @return a query parser that operates with the specified graph
     */
    public static QueryParser create(QueryBuilder queryBuilder) {
        QueryParser parser = new QueryParser(queryBuilder);
        parser.registerDefaultAggregates();
        return parser;
    }

    private void registerAggregate(String name, int numArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        registerAggregate(name, numArgs, numArgs, aggregateMethod);
    }

    private void registerAggregate(String name, int minArgs, int maxArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, args -> {
            if (args.size() < minArgs || args.size() > maxArgs) {
                throw GraqlQueryException.incorrectAggregateArgumentNumber(name, minArgs, maxArgs, args);
            }
            return aggregateMethod.apply(args);
        });
    }

    /**
     * Register an aggregate that can be used when parsing a Graql query
     *
     * @param name            the name of the aggregate
     * @param aggregateMethod a function that will produce an aggregate when passed a list of arguments
     */
    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
    }

    /**
     * Set whether the parser should set all {@link Var}s as user-defined. If it does, then every variable will
     * generate a user-defined name and be returned in results. For example:
     * <pre>
     *     match ($x, $y);
     * </pre>
     * <p>
     * The previous query would normally return only two concepts per result for {@code $x} and {@code $y}. However,
     * if the flag is set it will also return a third variable with a random name representing the relation
     * {@code $123 ($x, $y)}.
     */
    public void defineAllVars(boolean defineAllVars) {
        this.defineAllVars = defineAllVars;
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of QUERY.
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
     * @return a stream of query objects
     */
    public <T extends Query<?>> Stream<T> parseReader(Reader reader) {
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

    /**
     * @param queryString a string representing several queries
     * @return a stream of query objects
     */
    public <T extends Query<?>> Stream<T> parseList(String queryString) {
        return (Stream<T>) QUERY_LIST.parse(queryString);
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    public List<Pattern> parsePatterns(String patternsString) {
        return PATTERNS.parse(patternsString);
    }

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    public Pattern parsePattern(String patternString) {
        return PATTERN.parse(patternString);
    }

    /**
     * @param template a string representing a templated graql query
     * @param data     data to use in template
     * @return a resolved graql query
     */
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
    // TODO: remove this manual registration of aggregate queries and design it into the grammar
    @SuppressWarnings("unchecked")
    private void registerDefaultAggregates() {
        registerAggregate("count", 0, Integer.MAX_VALUE, args -> Graql.count());
        registerAggregate("sum", 1, args -> Graql.sum((Var) args.get(0)));
        registerAggregate("max", 1, args -> Graql.max((Var) args.get(0)));
        registerAggregate("min", 1, args -> Graql.min((Var) args.get(0)));
        registerAggregate("mean", 1, args -> Graql.mean((Var) args.get(0)));
        registerAggregate("median", 1, args -> Graql.median((Var) args.get(0)));
        registerAggregate("std", 1, args -> Graql.std((Var) args.get(0)));

        registerAggregate("group", 1, 2, args -> {
            if (args.size() < 2) {
                return Graql.group((Var) args.get(0));
            } else {
                return Graql.group((Var) args.get(0), (Aggregate) args.get(1));
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
