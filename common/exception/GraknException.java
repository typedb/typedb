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
 */

package grakn.core.common.exception;

/**
 * Root Grakn Exception
 * Encapsulates any exception which is thrown by the Grakn stack.
 * This includes failures server side, failed graph mutations, and failed querying attempts
 */
public abstract class GraknException extends RuntimeException {

    protected GraknException(String error) {
        super(error);
    }

    protected GraknException(String error, Exception e) {
        super(error, e);
    }

    protected GraknException(String error, Exception e, boolean enableSuppression, boolean writableStackTrace) {
        super(error, e, enableSuppression, writableStackTrace);
    }

    public abstract String getName();
}
