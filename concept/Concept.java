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

package grakn.core.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;

public interface Concept {

    boolean isDeleted();

    /**
     * Deletes this {@code Thing} from the system.
     */
    void delete();

    default Type asType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(Type.class)));
    }

    default Thing asThing() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Thing.class)));
    }

    default boolean isType() { return false; }

    default boolean isThing() { return false; }

    GraknException exception(GraknException exception);
}
