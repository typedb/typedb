/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.shell;

@SuppressWarnings("JavaDoc")
/**
 * Error message strings for shell
 */
public enum ErrorMessage {

    COULD_NOT_CREATE_TEMP_FILE("WARNING: could not create temporary file for editing queries");

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
