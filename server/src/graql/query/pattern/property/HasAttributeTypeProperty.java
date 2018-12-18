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

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

import static grakn.core.graql.internal.Schema.ImplicitType.KEY;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_VALUE;
import static grakn.core.graql.query.pattern.Pattern.var;

/**
 * Represents the {@code has} and {@code key} properties on a Type.
 * This property can be queried or inserted. Whether this is a key is indicated by the
 * HasAttributeTypeProperty#required field.
 * This property is defined as an implicit ontological structure between a Type and a AttributeType,
 * including one implicit RelationshipType and two implicit Roles. The labels of these types are derived
 * from the label of the AttributeType.
 * Like HasAttributeProperty, if this is not a key and is used in a match clause it will not use the implicit
 * structure - instead, it will match if there is any kind of relation type connecting the two types.
 */
public class HasAttributeTypeProperty extends VarProperty {

    private final Statement resourceType;
    private final Statement ownerRole;
    private final Statement valueRole;
    private final Statement relationOwner;
    private final Statement relationValue;
    private final boolean required;

    public HasAttributeTypeProperty(Statement resourceType, boolean required) {
        Label resourceLabel = resourceType.getTypeLabel().orElseThrow(
                () -> GraqlQueryException.noLabelSpecifiedForHas(resourceType)
        );

        Statement role = Pattern.label(Schema.MetaSchema.ROLE.getLabel());

        Statement ownerRole = var().sub(role);
        Statement valueRole = var().sub(role);
        Statement relationType = var().sub(Pattern.label(Schema.MetaSchema.RELATIONSHIP.getLabel()));

        // If a key, limit only to the implicit key type
        if (required) {
            ownerRole = ownerRole.label(KEY_OWNER.getLabel(resourceLabel));
            valueRole = valueRole.label(KEY_VALUE.getLabel(resourceLabel));
            relationType = relationType.label(KEY.getLabel(resourceLabel));
        }

        Statement relationOwner = relationType.relates(ownerRole);
        Statement relationValue = relationType.var().relates(valueRole);

        this.resourceType = resourceType;
        this.ownerRole = ownerRole;
        this.valueRole = valueRole;
        if (relationOwner == null) {
            throw new NullPointerException("Null relationOwner");
        }
        this.relationOwner = relationOwner;
        if (relationValue == null) {
            throw new NullPointerException("Null relationValue");
        }
        this.relationValue = relationValue;
        this.required = required;
    }

    public Statement type() {
        return resourceType;
    }

    @Override
    public String getName() {
        return required ? "key" : "has";
    }

    @Override
    public String getProperty() {
        return resourceType.getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        traversals.addAll(new PlaysProperty(ownerRole, required).match(start));
        //TODO: Get this to use real constraints no just the required flag
        traversals.addAll(new PlaysProperty(valueRole, false).match(resourceType.var()));
        traversals.addAll(new NeqProperty(ownerRole).match(valueRole.var()));

        return traversals;
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(resourceType);
    }

    @Override
    public Stream<Statement> innerStatements() {
        return Stream.of(resourceType);
    }

    @Override
    public Stream<Statement> implicitInnerStatements() {
        return Stream.of(resourceType, ownerRole, valueRole, relationOwner, relationValue);
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type entityTypeConcept = executor.get(var).asType();
            AttributeType attributeTypeConcept = executor.get(resourceType.var()).asAttributeType();

            if (required) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType.var()).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            AttributeType<?> attributeType = executor.get(resourceType.var()).asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (required) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, resourceType.var()).build());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof HasAttributeTypeProperty) {
            HasAttributeTypeProperty that = (HasAttributeTypeProperty) o;
            return (this.resourceType.equals(that.resourceType))
                    && (this.ownerRole.equals(that.ownerRole))
                    && (this.valueRole.equals(that.valueRole))
                    && (this.relationOwner.equals(that.relationOwner))
                    && (this.relationValue.equals(that.relationValue))
                    && (this.required == that.required);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.resourceType.hashCode();
        h *= 1000003;
        h ^= this.ownerRole.hashCode();
        h *= 1000003;
        h ^= this.valueRole.hashCode();
        h *= 1000003;
        h ^= this.relationOwner.hashCode();
        h *= 1000003;
        h ^= this.relationValue.hashCode();
        h *= 1000003;
        h ^= this.required ? 1231 : 1237;
        return h;
    }
}
