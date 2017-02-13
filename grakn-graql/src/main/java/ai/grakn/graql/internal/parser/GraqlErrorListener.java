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

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ANTLR error listener that listens for syntax errors.
 *
 * When a syntax error occurs, it is recorded. Call {@link GraqlErrorListener#hasErrors()} to see if there were errors.
 * View the errors with {@link GraqlErrorListener#toString()}.
 *
 * @author Felix Chapman
 */
public class GraqlErrorListener extends BaseErrorListener {

    private final List<String> query;
    private final List<SyntaxError> errors = new ArrayList<>();

    public GraqlErrorListener(String query) {
        if (query.isEmpty()) {
            this.query = null;
        } else {
            this.query = Lists.newArrayList(query.split("\n"));
        }
    }

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg,
            RecognitionException e) {

        if (query == null) {
            errors.add(new SyntaxError(line, msg));
        } else {
            errors.add(new SyntaxError(query.get(line-1), line, charPositionInLine, msg));
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

