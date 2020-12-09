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

package grakn.core.common.exception;

import grakn.common.collection.Pair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

import static grakn.common.collection.Collections.pair;

public class GraknException extends RuntimeException {
    @Nullable
    private final ErrorMessage errorMessage;
    @Nullable
    private final Object[] parameters;

    // TODO replace usages with GraknException.of()
    public GraknException(String error) {
        super(error);
        errorMessage = null;
        parameters = null;
    }

    // TODO replace usages with GraknException.of()
    public GraknException(ErrorMessage error, Object... parameters) {
        super(error.message(parameters));
        assert !getMessage().contains("%s");
        this.errorMessage = error;
        this.parameters = parameters;
    }

    // TODO replace usages with GraknException.of()
    public GraknException(Exception e) {
        super(e);
        errorMessage = null;
        parameters = null;
    }

    // TODO replace usages with GraknException.of()
    public GraknException(List<GraknException> exceptions) {
        super(getMessages(exceptions));
        errorMessage = null;
        parameters = null;
    }

    public static GraknException of(Exception e) {
        return new GraknException(e);
    }

    public static GraknException of(ErrorMessage errorMessage, Object... parameters) {
        return new GraknException(errorMessage, parameters);
    }

    public static GraknException of(String error) {
        return new GraknException(error);
    }

    public Optional<String> code() {
        return Optional.ofNullable(errorMessage).map(e -> e.code());
    }

    public static String getMessages(List<GraknException> exceptions) {
        final StringBuilder messages = new StringBuilder();
        for (GraknException exception : exceptions) {
            messages.append(exception.getMessage()).append("\n");
        }
        return messages.toString();
    }
}
