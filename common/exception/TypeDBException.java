/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

    private final ErrorMessage error;

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

    public ErrorMessage errorMessage() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeDBException that = (TypeDBException) o;
        return error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error);
    }
}
