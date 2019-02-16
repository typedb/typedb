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

package graql.lang.exception;

public enum ErrorMessage {
    SYNTAX_ERROR_NO_POINTER("syntax error at line %s:\n%s"),
    SYNTAX_ERROR("syntax error at line %s: \n%s\n%s\n%s"),
    CONFLICTING_PROPERTIES("the following unique properties in '%s' conflict: '%s' and '%s'"),
    VARIABLE_OUT_OF_SCOPE("the variable %s is out of scope of the query"),
    NO_PATTERNS("no patterns have been provided. at least one pattern must be provided"),
    INVALID_COMPUTE_METHOD("Invalid compute method. The available compute methods are: [%s]."),
    INVALID_COMPUTE_CONDITION("Invalid condition(s) for 'compute [%s]'. The accepted condition(s) are: [%s]."),
    MISSING_COMPUTE_CONDITION("Missing condition(s) for 'compute [%s]'. The required condition(s) are: [%s]."),
    INVALID_COMPUTE_METHOD_ALGORITHM("Invalid algorithm for 'compute [%s]'. The accepted algorithm(s) are: [%s]."),
    INVALID_COMPUTE_ARGUMENT("Invalid argument(s) 'compute [%s] using [%s]'. The accepted argument(s) are: [%s]."),;

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
