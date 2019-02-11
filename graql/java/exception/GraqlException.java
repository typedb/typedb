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


public class GraqlException extends RuntimeException {

    protected GraqlException(String error) {
        super(error);
    }

    protected GraqlException(String error, Exception e) {
        super(error, e);
    }

    public static GraqlException conflictingProperties(String statement, String property, String other) {
        return new GraqlException(graql.lang.exception.ErrorMessage.CONFLICTING_PROPERTIES.getMessage(statement, property, other));
    }

    public static GraqlException varNotInQuery(String var) {
        return new GraqlException(graql.lang.exception.ErrorMessage.VARIABLE_NOT_IN_QUERY.getMessage(var));
    }

    public static GraqlException noPatterns() {
        return new GraqlException(ErrorMessage.NO_PATTERNS.getMessage());
    }

    public String getName() {
        return this.getClass().getName();
    }
}