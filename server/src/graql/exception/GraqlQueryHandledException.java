/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;

/**
 *
 * Occurs when the query is syntactically correct but semantically incorrect.
 * Additionally, it is supposed to be caught and the processing resumed.
 *
 */
public class GraqlQueryHandledException extends GraknException {

    private final String NAME = "GraqlQueryHandledException";

    private GraqlQueryHandledException(String error) {
        super(error);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static GraqlQueryHandledException create(String formatString, Object... args) {
        return new GraqlQueryHandledException(String.format(formatString, args));
    }

    public static GraqlQueryHandledException idNotFound(ConceptId id) {
        return new GraqlQueryHandledException(ErrorMessage.ID_NOT_FOUND.getMessage(id));
    }

    public static GraqlQueryHandledException labelNotFound(Label label) {
        return new GraqlQueryHandledException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
    }
}
