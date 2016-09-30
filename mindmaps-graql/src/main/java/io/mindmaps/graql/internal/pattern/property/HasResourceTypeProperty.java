/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

public class HasResourceTypeProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin resourceType;

    private final VarAdmin ownerRole;
    private final VarAdmin valueRole;
    private final VarAdmin relationType;

    public HasResourceTypeProperty(VarAdmin resourceType) {
        this.resourceType = resourceType;

        String resourceTypeId = resourceType.getId().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_ID_SPECIFIED_FOR_HAS_RESOURCE.getMessage())
        );

        ownerRole = Graql.id(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId))
                .isa(Schema.MetaType.ROLE_TYPE.getId()).admin();
        valueRole = Graql.id(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId))
                .isa(Schema.MetaType.ROLE_TYPE.getId()).admin();

        relationType = Graql.id(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .isa(Schema.MetaType.RELATION_TYPE.getId())
                .hasRole(ownerRole).hasRole(valueRole).admin();
    }

    public VarAdmin getResourceType() {
        return resourceType;
    }

    @Override
    public String getName() {
        return "has-resource";
    }

    @Override
    public String getProperty() {
        return resourceType.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        Collection<MultiTraversal> traversals = new HashSet<>();

        PlaysRoleProperty ownerPlaysRole = new PlaysRoleProperty(ownerRole);
        traversals.addAll(ownerPlaysRole.getMultiTraversals(start));

        PlaysRoleProperty valuePlaysRole = new PlaysRoleProperty(valueRole);
        traversals.addAll(valuePlaysRole.getMultiTraversals(resourceType.getName()));

        return traversals;
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(resourceType);
    }

    @Override
    public Stream<VarAdmin> getImplicitInnerVars() {
        return Stream.of(resourceType, ownerRole, valueRole, relationType);
    }
}
