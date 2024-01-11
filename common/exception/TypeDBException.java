/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.common.exception;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * TODO: there is some technical debt in the way we throw and catch TypeDBException, which is a RuntimeException.
 * see issue #6021
 */
public class TypeDBException extends RuntimeException {

    @Nullable
    private final ErrorMessage error;

    private TypeDBException(String error) {
        super(error);
        this.error = null;
    }

    private TypeDBException(ErrorMessage error, Throwable cause) {
        super(error.message(cause), cause);
        assert !getMessage().contains("%s");
        this.error = error;
    }

    private TypeDBException(ErrorMessage error, Object... parameters) {
        super(error.message(parameters));
        assert !getMessage().contains("%s");
        this.error = error;
    }

    public static TypeDBException of(ErrorMessage errorMessage, Throwable cause) {
        return new TypeDBException(errorMessage, cause);
    }

    public static TypeDBException of(ErrorMessage errorMessage, Object... parameters) {
        return new TypeDBException(errorMessage, parameters);
    }

    public Optional<ErrorMessage> errorMessage() {
        return Optional.ofNullable(error);
    }

    public static TypeDBException of(List<TypeDBException> exceptions) {
        StringBuilder messages = new StringBuilder();
        for (TypeDBException exception : exceptions) {
            messages.append(exception.getMessage()).append("\n");
        }
        return new TypeDBException(messages.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeDBException that = (TypeDBException) o;
        return Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error);
    }
}
