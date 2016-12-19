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

import ai.grakn.util.ErrorMessage;
import org.apache.commons.lang.StringUtils;

class SyntaxError {
    private final String queryLine;
    private final int line;
    private final int charPositionInLine;
    private final String msg;

    SyntaxError(int line, String msg) {
        this.queryLine = null;
        this.line = line;
        this.charPositionInLine = 0;
        this.msg = msg;
    }

    SyntaxError(String queryLine, int line, int charPositionInLine, String msg) {
        this.queryLine = queryLine;
        this.line = line;
        this.charPositionInLine = charPositionInLine;
        this.msg = msg;
    }

    @Override
    public String toString() {
        if (queryLine == null) {
            return ErrorMessage.SYNTAX_ERROR_NO_POINTER.getMessage(line, msg);
        } else {
            // Error message appearance:
            //
            // syntax error at line 1:
            // match $
            //       ^
            // blah blah antlr blah
            String pointer = StringUtils.repeat(" ", charPositionInLine) + "^";
            return ErrorMessage.SYNTAX_ERROR.getMessage(line, queryLine, pointer, msg);
        }
    }
}
