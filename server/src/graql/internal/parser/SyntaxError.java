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

package grakn.core.graql.internal.parser;

import grakn.core.common.exception.ErrorMessage;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;

@AutoValue
abstract class SyntaxError {
    abstract @Nullable String queryLine();
    abstract int line();
    abstract int charPositionInLine();
    abstract String msg();

    static SyntaxError of(int line, String msg) {
        return of(line, msg, null, 0);
    }

    static SyntaxError of(int line, String msg, @Nullable String queryLine, int charPositionInLine) {
        return new AutoValue_SyntaxError(queryLine, line, charPositionInLine, msg);
    }

    @Override
    public String toString() {
        if (queryLine() == null) {
            return ErrorMessage.SYNTAX_ERROR_NO_POINTER.getMessage(line(), msg());
        } else {
            // Error message appearance:
            //
            // syntax error at line 1:
            // match $
            //       ^
            // blah blah antlr blah
            String pointer = StringUtils.repeat(" ", charPositionInLine()) + "^";
            return ErrorMessage.SYNTAX_ERROR.getMessage(line(), queryLine(), pointer, msg());
        }
    }
}
