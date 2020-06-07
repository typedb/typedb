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

package grakn.core.graql.analytics;

import grakn.core.common.exception.GraknException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

public class GraknAnalyticsException extends GraknException {

    GraknAnalyticsException(String error) {
        super(error);
    }

    GraknAnalyticsException(String error, Exception e) {
        super(error, e);
    }

    @CheckReturnValue
    public static GraknAnalyticsException unreachableStatement(Exception cause) {
        return unreachableStatement(null, cause);
    }

    @CheckReturnValue
    public static GraknAnalyticsException unreachableStatement(String message) {
        return unreachableStatement(message, null);
    }

    @CheckReturnValue
    private static GraknAnalyticsException unreachableStatement(@Nullable String message, Exception cause) {
        return new GraknAnalyticsException("Statement expected to be unreachable: " + message, cause);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknAnalyticsException create(String error) {
        return new GraknAnalyticsException(error);
    }
}
