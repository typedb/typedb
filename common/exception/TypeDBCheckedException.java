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

import javax.annotation.Nullable;
import java.util.Optional;

public class TypeDBCheckedException extends Exception {

    @Nullable
    private final ErrorMessage errorMessage;

    private TypeDBCheckedException(String error) {
        super(error);
        errorMessage = null;
    }

    private TypeDBCheckedException(ErrorMessage error, Object... parameters) {
        super(error.message(parameters));
        assert !getMessage().contains("%s");
        this.errorMessage = error;
    }

    private TypeDBCheckedException(Exception e) {
        super(e);
        errorMessage = null;
    }

    public static TypeDBCheckedException of(Exception e) {
        return new TypeDBCheckedException(e);
    }

    public static TypeDBCheckedException of(ErrorMessage errorMessage, Object... parameters) {
        return new TypeDBCheckedException(errorMessage, parameters);
    }

    public Optional<String> code() {
        return Optional.ofNullable(errorMessage).map(com.vaticle.typedb.common.exception.ErrorMessage::code);
    }
}
