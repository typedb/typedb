/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */


package grakn.core.kb.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import graql.lang.statement.Statement;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * Runtime exception signalling illegal states of the system encountered during query processing.
 */
public class GraqlQueryException extends GraknException {

    private final String NAME = "GraqlQueryException";

    private GraqlQueryException(String error) { super(error); }

    private GraqlQueryException(String error, Exception e) {
        super(error, e);
    }


    @Override
    public String getName() { return NAME; }

    public static GraqlQueryException maxIterationsReached(Class<?> clazz) {
        return new GraqlQueryException(ErrorMessage.MAX_ITERATION_REACHED.getMessage(clazz.toString()));
    }

    public static GraqlQueryException nonRoleIdAssignedToRoleVariable(Statement var) {
        return new GraqlQueryException(ErrorMessage.ROLE_ID_IS_NOT_ROLE.getMessage(var.toString()));
    }

    @CheckReturnValue
    public static GraqlQueryException unreachableStatement(Exception cause) {
        return unreachableStatement(null, cause);
    }

    @CheckReturnValue
    public static GraqlQueryException unreachableStatement(String message) {
        return unreachableStatement(message, null);
    }

    @CheckReturnValue
    private static GraqlQueryException unreachableStatement(@Nullable String message, Exception cause) {
        return new GraqlQueryException("Statement expected to be unreachable: " + message, cause);
    }
}
