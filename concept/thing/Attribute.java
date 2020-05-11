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

package hypergraph.concept.thing;

import hypergraph.concept.type.AttributeType;

public interface Attribute extends Thing {

    /**
     * Get the immediate {@code AttributeType} in which this this {@code Attribute} is an instance of.
     *
     * @return the {@code AttributeType} of this {@code Attribute}
     */
    @Override
    AttributeType type();

    /**
     * Set an {@code Attribute} to be owned by this {@code Attribute}.
     *
     * @param attribute that will be owned by this {@code Attribute}
     * @return this {@code Attribute} for further manipulation
     */
    @Override
    Attribute has(Attribute attribute);
}
