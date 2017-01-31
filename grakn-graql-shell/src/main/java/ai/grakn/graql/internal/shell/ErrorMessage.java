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

package ai.grakn.graql.internal.shell;

/**
 * Error message strings for shell
 *
 * @author Felix Chapman
 */
public enum ErrorMessage {

    COULD_NOT_CREATE_TEMP_FILE("WARNING: could not create temporary file for editing queries"),
    COULD_NOT_CONNECT("Could not connect to Grakn Engine. Have you run 'grakn.sh start'?");

    private final String message;

    /**
     * @param message the error message string, with parameters defined using %s
     */
    ErrorMessage(String message) {
        this.message = message;
    }

    /**
     * @param args arguments to substitute into the message
     * @return the error message string, with arguments substituted
     */
    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
