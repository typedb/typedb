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

import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.Pattern;
import graql.grammar.GraqlLexer;
import graql.grammar.GraqlParser;
import graql.parser.ErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.List;
import java.util.stream.Stream;

/**
 * Graql query string parser to produce Graql Java objects
 */
public class Parser {

    public Parser() {}

    private GraqlParser parse(String queryString, ErrorListener errorListener) {
        CharStream charStream = CharStreams.fromString(queryString);

        GraqlLexer lexer = new GraqlLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GraqlParser parser = new GraqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        return parser;
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<?>> T parseQueryEOF(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        GraqlParser.QueryEOFContext queryEOFContext = parser.queryEOF();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return (T) new Visitor().visitQueryEOF(queryEOFContext);
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<?>> Stream<T> parseList(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        GraqlParser.QueryListContext queryListContext = parser.queryList();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return (Stream<T>) new Visitor().visitQueryList(queryListContext);
    }

    public List<Pattern> parsePatterns(String patternsString) {
        ErrorListener errorListener = ErrorListener.of(patternsString);
        GraqlParser parser = parse(patternsString, errorListener);

        GraqlParser.PatternsContext patternsContext = parser.patterns();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return new Visitor().visitPatterns(patternsContext);
    }

    public Pattern parsePattern(String patternString) {
        ErrorListener errorListener = ErrorListener.of(patternString);
        GraqlParser parser = parse(patternString, errorListener);

        GraqlParser.PatternContext patternContext = parser.pattern();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return new Visitor().visitPattern(patternContext);
    }
}
