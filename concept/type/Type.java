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

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;

import java.util.List;
import java.util.stream.Stream;

public interface Type extends Concept {

    Long count();

    boolean isRoot();

    void setLabel(String label);

    Label getLabel();

    boolean isAbstract();

    Type getSupertype();

    Stream<? extends Type> getSupertypes();

    Stream<? extends Type> getSubtypes();

    List<GraknException> validate();

    ThingType asThingType();

    EntityType asEntityType();

    AttributeType asAttributeType();

    RelationType asRelationType();

    RoleType asRoleType();
}
