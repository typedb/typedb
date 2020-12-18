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

package grakn.core.concept.type;

import grakn.core.concept.thing.Relation;

import java.util.stream.Stream;

public interface RelationType extends ThingType {

    @Override
    Stream<? extends RelationType> getSubtypes();

    @Override
    Stream<? extends RelationType> getSubtypesExplicit();

    @Override
    Stream<? extends Relation> getInstances();

    void setSupertype(RelationType superType);

    void setRelates(String roleLabel);

    void setRelates(String roleLabel, String overriddenLabel);

    void unsetRelates(String roleLabel);

    Stream<? extends RoleType> getRelates();

    Stream<? extends RoleType> getRelatesExplicit();

    RoleType getRelates(String roleLabel);

    RoleType getRelatesExplicit(String roleLabel);

    RoleType getRelatesOverridden(String roleLabel);

    Relation create();

    Relation create(boolean isInferred);
}
