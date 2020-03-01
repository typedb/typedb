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

package grakn.core.kb.concept.structure;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import static grakn.core.common.exception.ErrorMessage.INVALID_DIRECTION;

public class GraknElementException  extends GraknException {

    GraknElementException(String error) {
        super(error);
    }

    protected GraknElementException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknElementException create(String error) {
        return new GraknElementException(error);
    }


    /**
     * Thrown when attempting to traverse an edge in an invalid direction
     */
    public static GraknElementException invalidDirection(Direction direction) {
        return create(INVALID_DIRECTION.getMessage(direction));
    }

    /**
     * Thrown when trying to build a Concept from an invalid vertex or edge
     */
    public static GraknElementException invalidElement(Element element) {
        return create(String.format("Cannot build a concept from element {%s} due to it being deleted.", element));
    }


    /**
     * Thrown when attempting to mutate a property which is immutable
     */
    public static GraknElementException immutableProperty(Object oldValue, Object newValue, Enum vertexProperty) {
        return create(ErrorMessage.IMMUTABLE_VALUE.getMessage(oldValue, newValue, vertexProperty.name()));
    }

}
