/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.reasoner.atom.binary.HasAtom;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.ImplicitType.KEY;
import static ai.grakn.util.Schema.ImplicitType.KEY_OWNER;
import static ai.grakn.util.Schema.ImplicitType.KEY_VALUE;

/**
 * Represents the {@code has} and {@code key} properties on a {@link Type}.
 *
 * This property can be queried or inserted. Whether this is a key is indicated by the
 * {@link HasAttributeTypeProperty#required} field.
 *
 * This property is defined as an implicit ontological structure between a {@link Type} and a {@link AttributeType},
 * including one implicit {@link RelationshipType} and two implicit {@link Role}s. The labels of these types are derived
 * from the label of the {@link AttributeType}.
 *
 * Like {@link HasAttributeProperty}, if this is not a key and is used in a {@link Match} it will not use the implicit
 * structure - instead, it will match if there is any kind of relation type connecting the two types.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class HasAttributeTypeProperty extends AbstractVarProperty implements NamedProperty {

    abstract VarPatternAdmin resourceType();

    abstract VarPatternAdmin ownerRole();
    abstract VarPatternAdmin valueRole();
    abstract VarPatternAdmin relationOwner();
    abstract VarPatternAdmin relationValue();

    abstract boolean required();

    /**
     * @throws GraqlQueryException if no label is specified on {@code resourceType}
     */
    public static HasAttributeTypeProperty of(VarPatternAdmin resourceType, boolean required) {
        Label resourceLabel = resourceType.getTypeLabel().orElseThrow(() ->
                GraqlQueryException.noLabelSpecifiedForHas(resourceType)
        );

        VarPattern role = Graql.label(Schema.MetaSchema.ROLE.getLabel());

        VarPatternAdmin ownerRole = var().sub(role).admin();
        VarPatternAdmin valueRole = var().sub(role).admin();
        VarPattern relationType = var().sub(Graql.label(Schema.MetaSchema.RELATIONSHIP.getLabel()));

        // If a key, limit only to the implicit key type
        if(required){
            ownerRole = ownerRole.label(KEY_OWNER.getLabel(resourceLabel)).admin();
            valueRole = valueRole.label(KEY_VALUE.getLabel(resourceLabel)).admin();
            relationType = relationType.label(KEY.getLabel(resourceLabel));
        }

        VarPatternAdmin relationOwner = relationType.relates(ownerRole).admin();
        VarPatternAdmin relationValue = relationType.admin().var().relates(valueRole).admin();

        return new AutoValue_HasAttributeTypeProperty(
                resourceType, ownerRole, valueRole, relationOwner, relationValue, required);
    }

    @Override
    public String getName() {
        return required() ? "key" : "has";
    }

    @Override
    public String getProperty() {
        return resourceType().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        traversals.addAll(PlaysProperty.of(ownerRole(), required()).match(start));
        //TODO: Get this to use real constraints no just the required flag
        traversals.addAll(PlaysProperty.of(valueRole(), false).match(resourceType().var()));
        traversals.addAll(NeqProperty.of(ownerRole()).match(valueRole().var()));

        return traversals;
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(resourceType());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(resourceType());
    }

    @Override
    public Stream<VarPatternAdmin> implicitInnerVarPatterns() {
        return Stream.of(resourceType(), ownerRole(), valueRole(), relationOwner(), relationValue());
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type entityTypeConcept = executor.get(var).asType();
            AttributeType attributeTypeConcept = executor.get(resourceType().var()).asAttributeType();

            if (required()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.attribute(attributeTypeConcept);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType().var()).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            AttributeType<?> attributeType = executor.get(resourceType().var()).asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (required()) {
                    type.deleteKey(attributeType);
                } else {
                    type.deleteAttribute(attributeType);
                }
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType().var()).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        Var varName = var.var().asUserDefined();
        Label label = this.resourceType().getTypeLabel().orElse(null);

        Var predicateVar = var().asUserDefined();
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(label);
        ConceptId predicateId = schemaConcept != null? schemaConcept.getId() : null;
        //isa part
        VarPatternAdmin resVar = varName.has(Graql.label(label)).admin();
        return HasAtom.create(resVar, predicateVar, predicateId, parent);
    }
}
