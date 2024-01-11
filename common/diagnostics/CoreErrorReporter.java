/*
 * Copyright (C) 2023 Vaticle
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

package com.vaticle.typedb.core.common.diagnostics;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import io.sentry.ITransaction;
import io.sentry.Sentry;
import io.sentry.SpanStatus;

public class CoreErrorReporter implements ErrorReporter {

    public void reportError(Throwable error) {
        if (error instanceof TypeDBException) {
            TypeDBException exception = (TypeDBException) error;
            submitTypeDBException(exception);
        } else {
            Sentry.captureException(error);
        }
    }

    private void submitTypeDBException(TypeDBException exception) {
        if (exception.errorMessage().isPresent()) {
            if (!exception.errorMessage().get().getClass().equals(ErrorMessage.Internal.class)) {
                ITransaction txn = Sentry.startTransaction("user_error", "user_error");
                txn.setData("error_code", exception.errorMessage().get().code());
                txn.finish(SpanStatus.OK);
            } else {
                Sentry.captureException(exception);
            }
        } else if (exception.getCause() != null) {
            Throwable cause = exception.getCause();
            if (cause instanceof TypeDBException) {
                // unwrap the recursive TypeDB exceptions to get to the root cause - we should eliminate these cases
                submitTypeDBException((TypeDBException) cause);
            } else {
                Sentry.captureException(cause);
            }
        }
    }
}
