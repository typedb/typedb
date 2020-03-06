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

package grakn.core.kb.server.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

import java.util.List;

/**
 * <p>
 *     Broken Knowledge Base Exception
 * </p>
 *
 * <p>
 *     This exception is thrown on Transaction#commit() when the graph does not comply with the grakn
 *     validation rules. For a complete list of these rules please refer to the documentation
 * </p>
 *
 */
public class InvalidKBException extends GraknException {

    private final String NAME = "InvalidKBException";

    private InvalidKBException(String message) {
        super(message);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static InvalidKBException create(String message) {
        return new InvalidKBException(message);
    }

    /**
     * Thrown on commit when validation errors are found
     */
    public static InvalidKBException validationErrors(List<String> errors){
        StringBuilder message = new StringBuilder();
        message.append(ErrorMessage.VALIDATION.getMessage(errors.size()));
        for (String s : errors) {
            message.append(s);
        }
        return create(message.toString());
    }

}
