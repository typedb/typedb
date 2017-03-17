/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.name;

/**
 * Represents the {@code has-resource} and {@code has-key} properties on a {@link Type}.
 *
 * This property can be queried or inserted. Whether this is a key is indicated by the
 * {@link HasResourceTypeProperty#required} field.
 *
 * This property is defined as an implicit ontological structure between a {@link Type} and a {@link ResourceType},
 * including one implicit {@link RelationType} and two implicit {@link RoleType}s. The names of these types are derived
 * from the name of the {@link ResourceType}.
 *
 * @author Felix Chapman
 */
public class HasResourceTypeProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin resourceType;

    private final VarAdmin ownerRole;
    private final VarAdmin valueRole;
    private final VarAdmin relationType;

    private final PlaysRoleProperty ownerPlaysRole;

    private final boolean required;

    public HasResourceTypeProperty(VarAdmin resourceType, boolean required) {
        this.resourceType = resourceType;
        this.required = required;

        TypeName resourceTypeName = resourceType.getTypeName().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_NAME_SPECIFIED_FOR_HAS_RESOURCE.getMessage())
        );

        Var role = name(Schema.MetaSchema.ROLE.getName());

        //Choose the correct implicit type
        Schema.ImplicitType has;
        Schema.ImplicitType hasOwner;
        Schema.ImplicitType hasValue;
        if(required){
            has = Schema.ImplicitType.HAS_KEY;
            hasOwner = Schema.ImplicitType.HAS_KEY_OWNER;
            hasValue = Schema.ImplicitType.HAS_KEY_VALUE;
        } else {
            has = Schema.ImplicitType.HAS_RESOURCE;
            hasOwner = Schema.ImplicitType.HAS_RESOURCE_OWNER;
            hasValue = Schema.ImplicitType.HAS_KEY_VALUE;
        }

        ownerRole = name(hasOwner.getName(resourceTypeName)).sub(role).admin();
        valueRole = name(hasValue.getName(resourceTypeName)).sub(role).admin();

        relationType = name(has.getName(resourceTypeName))
                .sub(name(Schema.MetaSchema.RELATION.getName()))
                .hasRole(ownerRole).hasRole(valueRole).admin();

        ownerPlaysRole = new PlaysRoleProperty(ownerRole, required);
    }

    public VarAdmin getResourceType() {
        return resourceType;
    }

    @Override
    public String getName() {
        return required ? "has-key" : "has-resource";
    }

    @Override
    public String getProperty() {
        return resourceType.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();
        
        traversals.addAll(ownerPlaysRole.match(start));
        //TODO: Get this to use real constraints no just the required flag
        PlaysRoleProperty valuePlaysRole = new PlaysRoleProperty(valueRole, false);
        traversals.addAll(valuePlaysRole.match(resourceType.getVarName()));

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
        Type entityTypeConcept = concept.asType();
        ResourceType resourceTypeConcept = insertQueryExecutor.getConcept(resourceType).asResourceType();

        if (required) {
            entityTypeConcept.key(resourceTypeConcept);
        } else {
            entityTypeConcept.hasResource(resourceTypeConcept);
        }
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

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        //TODO NB: HasResourceType is a special case and it doesn't allow variables as resource types
        VarName varName = var.getVarName();
        TypeName typeName = this.getResourceType().getTypeName().orElse(null);
        //isa part
        VarAdmin resVar = Graql.var(varName).hasResource(name(typeName)).admin();
        return new TypeAtom(resVar, parent);
    }
}
