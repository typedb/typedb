/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.concept.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

public class GraknConceptException extends GraknException {

    GraknConceptException(String error) {
        super(error);
    }

    protected GraknConceptException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknConceptException create(String error) {
        return new GraknConceptException(error);
    }

    /**
     * Thrown when casting Grakn concepts/answers incorrectly.
     */
    public static GraknConceptException invalidCasting(Object concept, Class type) {
        return GraknConceptException.create(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }

    public static GraknConceptException variableDoesNotExist(String var) {
        return new GraknConceptException(ErrorMessage.VARIABLE_DOES_NOT_EXIST.getMessage(var));
    }
}
