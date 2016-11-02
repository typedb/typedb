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

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.EquivalentFragmentSet;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
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

    private final PlaysRoleProperty ownerPlaysRole;
    private final PlaysRoleProperty valuePlaysRole;

    public HasResourceTypeProperty(VarAdmin resourceType) {
        this.resourceType = resourceType;

        String resourceTypeId = resourceType.getId().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_ID_SPECIFIED_FOR_HAS_RESOURCE.getMessage())
        );

        ownerRole = Graql.id(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId))
                .isa(Schema.MetaSchema.ROLE_TYPE.getId()).admin();
        valueRole = Graql.id(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId))
                .isa(Schema.MetaSchema.ROLE_TYPE.getId()).admin();

        relationType = Graql.id(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId))
                .isa(Schema.MetaSchema.RELATION_TYPE.getId())
                .hasRole(ownerRole).hasRole(valueRole).admin();

        ownerPlaysRole = new PlaysRoleProperty(ownerRole);
        valuePlaysRole = new PlaysRoleProperty(valueRole);
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
    public Collection<EquivalentFragmentSet> match(String start) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        traversals.addAll(ownerPlaysRole.match(start));

        PlaysRoleProperty valuePlaysRole = new PlaysRoleProperty(valueRole);
        traversals.addAll(valuePlaysRole.match(resourceType.getName()));

        return traversals;
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(resourceType);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(resourceType);
    }

    @Override
    public Stream<VarAdmin> getImplicitInnerVars() {
        return Stream.of(resourceType, ownerRole, valueRole, relationType);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        EntityType entityTypeConcept = concept.asEntityType();
        ResourceType resourceTypeConcept = insertQueryExecutor.getConcept(resourceType).asResourceType();
        entityTypeConcept.hasResource(resourceTypeConcept);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasResourceTypeProperty that = (HasResourceTypeProperty) o;

        return resourceType.equals(that.resourceType);

    }

    @Override
    public int hashCode() {
        return resourceType.hashCode();
    }
}
