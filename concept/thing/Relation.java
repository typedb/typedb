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

import hypergraph.concept.type.RelationType;

public interface Relation extends Thing {

    /**
     * Cast the {@code Concept} down to {@code Relation}
     *
     * @return this {@code Relation}
     */
    @Override
    default Relation asRelation() { return this; }

    /**
     * Get the immediate {@code RelationType} in which this this {@code Relation} is an instance of.
     *
     * @return the {@code RelationType} of this {@code Relation}
     */
    @Override
    RelationType type();

    /**
     * Set an {@code Attribute} to be owned by this {@code Relation}.
     *
     * @param attribute that will be owned by this {@code Relation}
     * @return this {@code Relation} for further manipulation
     */
    @Override
    Relation has(Attribute attribute);

}
