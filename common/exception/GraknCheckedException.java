/*
 * Copyright (C) 2021 Grakn Labs
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

import javax.annotation.Nullable;
import java.util.Optional;

public class GraknCheckedException extends Exception {

    @Nullable
    private final ErrorMessage errorMessage;

    private GraknCheckedException(String error) {
        super(error);
        errorMessage = null;
    }

    private GraknCheckedException(ErrorMessage error, Object... parameters) {
        super(error.message(parameters));
        assert !getMessage().contains("%s");
        this.errorMessage = error;
    }

    private GraknCheckedException(Exception e) {
        super(e);
        errorMessage = null;
    }

    public static GraknCheckedException of(Exception e) {
        return new GraknCheckedException(e);
    }

    public static GraknCheckedException of(ErrorMessage errorMessage, Object... parameters) {
        return new GraknCheckedException(errorMessage, parameters);
    }

    public Optional<String> code() {
        return Optional.ofNullable(errorMessage).map(grakn.common.exception.ErrorMessage::code);
    }
}
