/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.reasoner.atom.binary.HasAtom;
import grakn.core.graql.internal.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_VALUE;

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
 */
@AutoValue
public abstract class HasAttributeTypeProperty extends AbstractVarProperty implements NamedProperty {

    abstract Statement resourceType();

    abstract Statement ownerRole();
    abstract Statement valueRole();
    abstract Statement relationOwner();
    abstract Statement relationValue();

    abstract boolean required();

    /**
     * @throws GraqlQueryException if no label is specified on {@code resourceType}
     */
    public static HasAttributeTypeProperty of(Statement resourceType, boolean required) {
        Label resourceLabel = resourceType.getTypeLabel().orElseThrow(() ->
                GraqlQueryException.noLabelSpecifiedForHas(resourceType)
        );

        Statement role = Pattern.label(Schema.MetaSchema.ROLE.getLabel());

        Statement ownerRole = var().sub(role);
        Statement valueRole = var().sub(role);
        Statement relationType = var().sub(Pattern.label(Schema.MetaSchema.RELATIONSHIP.getLabel()));

        // If a key, limit only to the implicit key type
        if(required){
            ownerRole = ownerRole.label(KEY_OWNER.getLabel(resourceLabel));
            valueRole = valueRole.label(KEY_VALUE.getLabel(resourceLabel));
            relationType = relationType.label(KEY.getLabel(resourceLabel));
        }

        Statement relationOwner = relationType.relates(ownerRole);
        Statement relationValue = relationType.var().relates(valueRole);

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
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        traversals.addAll(PlaysProperty.of(ownerRole(), required()).match(start));
        //TODO: Get this to use real constraints no just the required flag
        traversals.addAll(PlaysProperty.of(valueRole(), false).match(resourceType().var()));
        traversals.addAll(NeqProperty.of(ownerRole()).match(valueRole().var()));

        return traversals;
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(resourceType());
    }

    @Override
    public Stream<Statement> innerVarPatterns() {
        return Stream.of(resourceType());
    }

    @Override
    public Stream<Statement> implicitInnerVarPatterns() {
        return Stream.of(resourceType(), ownerRole(), valueRole(), relationOwner(), relationValue());
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type entityTypeConcept = executor.get(var).asType();
            AttributeType attributeTypeConcept = executor.get(resourceType().var()).asAttributeType();

            if (required()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType().var()).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            AttributeType<?> attributeType = executor.get(resourceType().var()).asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (required()) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType().var()).build());
    }

    @Override
    public Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        Variable varName = var.var().asUserDefined();
        Label label = this.resourceType().getTypeLabel().orElse(null);

        Variable predicateVar = var();
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(label);
        ConceptId predicateId = schemaConcept != null? schemaConcept.id() : null;
        //isa part
        Statement resVar = varName.has(Pattern.label(label));
        return HasAtom.create(resVar, predicateVar, predicateId, parent);
    }
}
