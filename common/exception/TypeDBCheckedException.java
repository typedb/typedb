/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
