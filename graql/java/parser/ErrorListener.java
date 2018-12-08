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

package graql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ANTLR error listener that listens for syntax errors.
 * When a syntax error occurs, it is recorded. Call {@link ErrorListener#hasErrors()} to see if there were errors.
 * View the errors with {@link ErrorListener#toString()}.
 */
public class ErrorListener extends BaseErrorListener {

    private final List<String> query;
    private final List<SyntaxError> errors = new ArrayList<>();

    private ErrorListener(List<String> query) {
        this.query = query;
    }

    /**
     * Create a {@link ErrorListener} without a reference to a query string.
     * This will have limited error-reporting abilities, but is necessary when dealing with very large queries
     * that should not be held in memory all at once.
     */
    public static ErrorListener withoutQueryString() {
        return new ErrorListener(null);
    }

    public static ErrorListener of(String query) {
        List<String> queryList = Arrays.asList(query.split("\n"));
        return new ErrorListener(queryList);
    }

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg,
            RecognitionException e) {

        if (query == null) {
            errors.add(new SyntaxError(null, line, 0, msg));
        } else {
            errors.add(new SyntaxError(query.get(line - 1), line, charPositionInLine, msg));
        }
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public String toString() {
        return errors.stream().map(SyntaxError::toString).collect(Collectors.joining("\n"));
    }
}

