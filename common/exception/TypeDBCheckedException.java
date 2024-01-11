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

import java.util.Optional;

public class TypeDBCheckedException extends Exception {

    private final ErrorMessage errorMessage;
    private final Object[] parameters;


    private TypeDBCheckedException(ErrorMessage error, Object... parameters) {
        super(error.message(parameters));
        assert !getMessage().contains("%s");
        this.errorMessage = error;
        this.parameters = parameters;
    }

    public static TypeDBCheckedException of(ErrorMessage errorMessage, Object... parameters) {
        return new TypeDBCheckedException(errorMessage, parameters);
    }

    public Optional<ErrorMessage> errorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    public TypeDBException toUnchecked() {
        return TypeDBException.of(errorMessage, parameters);
    }

}
