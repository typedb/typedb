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

package grakn.core.query.writer;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.concept.Concepts;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import graql.lang.pattern.property.TypeProperty;
import graql.lang.pattern.variable.Identity;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlDefine;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MODIFIED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_DEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_NOT_EXIST;

public class DefineWriter {

    private final Concepts concepts;
    private final Context.Query context;
    private final Set<Identity> visited;
    private final List<Type> defined;
    private Map<Identity.Label, TypeVariable> variables;

    public DefineWriter(Concepts concepts, GraqlDefine query, Context.Query context) {
        this.concepts = concepts;
        this.context = context;
        this.variables = query.asGraph();
        this.visited = new HashSet<>();
        this.defined = new LinkedList<>();
    }

    public List<Type> write() {
        variables.keySet().forEach(identity -> {
            if (!visited.contains(identity)) define(identity);
        });
        return list(defined);
    }

    private Type define(Identity.Label identity) {
        TypeVariable variable = variables.get(identity);
        assert variable.labelProperty().isPresent();
        TypeProperty.Label labelProperty = variable.labelProperty().get();

        if (labelProperty.scope().isPresent() && variable.properties().size() > 1) {
            throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(labelProperty.scopedLabel()));
        } else if (labelProperty.scope().isPresent()) return null; // do nothing
        else if (visited.contains(identity)) return concepts.getType(labelProperty.scopedLabel());

        ThingType type = getTypeByLabel(labelProperty);
        if (variable.subProperty().isPresent()) {
            type = defineSub(type, labelProperty, variable.subProperty().get(),
                             variable.valueTypeProperty().orElse(null));
        } else if (variable.valueTypeProperty().isPresent()) {
            throw new GraknException(ATTRIBUTE_VALUE_TYPE_MODIFIED.message(
                    variable.valueTypeProperty().get().valueType().name(),
                    variable.labelProperty().get().label()
            ));
        } else if (type == null) {
            throw new GraknException(TYPE_NOT_EXIST.message(labelProperty.label()));
        }

        if (variable.abstractProperty().isPresent()) defineAbstract(type);
        if (variable.regexProperty().isPresent())
            defineRegex(type.asAttributeType().asString(), variable.regexProperty().get());
        // TODO: if (variable.whenProperty().isPresent()) defineWhen(variable);
        // TODO: if (variable.thenProperty().isPresent()) defineThen(variable);

        if (!variable.relatesProperty().isEmpty()) defineRelates(type.asRelationType(), variable.relatesProperty());
        if (!variable.ownsProperty().isEmpty()) defineOwns(type, variable.ownsProperty());
        if (!variable.playsProperty().isEmpty()) definePlays(type, variable.playsProperty());

        // if this variable had more properties beyond being referred to using a label, add to list of defined types
        if (variable.properties().size() > 1) defined.add(type);
        visited.add(identity);
        return type;
    }

    private ThingType getTypeByLabel(TypeProperty.Label label) {
        Type type;
        if ((type = concepts.getType(label.label())) != null) return type.asThingType();
        else return null;
    }

    private RoleType getTypeByScopedLabel(TypeProperty.Label label) {
        // We always assume that Role Types already exist,
        // defined by their Relation Types ahead of time
        assert label.scope().isPresent();
        Type type;
        RoleType roleType;
        if ((type = concepts.getType(label.scope().get())) == null ||
                (roleType = type.asRelationType().getRelates(label.label())) == null) {
            throw new GraknException(TYPE_NOT_EXIST.message(label.scopedLabel()));
        }
        return roleType;
    }

    private AttributeType.ValueType getValueType(TypeProperty.ValueType valueTypeProperty) {
        return AttributeType.ValueType.of(valueTypeProperty.valueType());
    }

    private ThingType defineSub(ThingType thingType, TypeProperty.Label labelProperty,
                                TypeProperty.Sub subProperty, @Nullable TypeProperty.ValueType valueTypeProperty) {
        ThingType supertype = define(subProperty.type().identity()).asThingType();
        if (supertype instanceof EntityType) {
            if (thingType == null) thingType = concepts.putEntityType(labelProperty.label());
            thingType.asEntityType().setSupertype(supertype.asEntityType());
        } else if (supertype instanceof RelationType) {
            if (thingType == null) thingType = concepts.putRelationType(labelProperty.label());
            thingType.asRelationType().setSupertype(supertype.asRelationType());
        } else if (supertype instanceof AttributeType) {
            AttributeType.ValueType valueType;
            if (valueTypeProperty != null) {
                valueType = getValueType(valueTypeProperty);
            } else if (!supertype.isRoot()) {
                valueType = supertype.asAttributeType().getValueType();
            } else {
                throw new GraknException(ATTRIBUTE_VALUE_TYPE_MISSING.message(labelProperty.label()));
            }
            thingType = concepts.putAttributeType(labelProperty.label(), valueType);
            thingType.asAttributeType().setSupertype(supertype.asAttributeType());
        } else {
            throw new GraknException(INVALID_DEFINE_SUB.message(
                    labelProperty.scopedLabel(), supertype.asRoleType().getScopedLabel()
            ));
        }
        return thingType;
    }

    private void defineAbstract(ThingType thingType) {
        thingType.setAbstract();
    }

    private void defineRegex(AttributeType.String attributeType, TypeProperty.Regex regexProperty) {
        attributeType.setRegex(regexProperty.regex());
    }

    private void defineRelates(RelationType relationType, List<TypeProperty.Relates> relatesProperties) {
        relatesProperties.forEach(relates -> {
            String roleTypeLabel = relates.role().labelProperty().get().label();
            if (relates.overridden().isPresent()) {
                String overriddenTypeLabel = relates.overridden().get().labelProperty().get().label();
                relationType.setRelates(roleTypeLabel, overriddenTypeLabel);
                visited.add(relates.overridden().get().identity());
            } else {
                relationType.setRelates(roleTypeLabel);
            }
            visited.add(relates.role().identity());
        });
    }

    private void defineOwns(ThingType thingType, List<TypeProperty.Owns> ownsProperties) {
        ownsProperties.forEach(owns -> {
            AttributeType attributeType = define(owns.attribute().identity()).asAttributeType();
            if (owns.overridden().isPresent()) {
                AttributeType overriddenType = define(owns.overridden().get().identity()).asAttributeType();
                thingType.setOwns(attributeType, overriddenType, owns.isKey());
            } else {
                thingType.setOwns(attributeType, owns.isKey());
            }
        });
    }

    private void definePlays(ThingType thingType, List<TypeProperty.Plays> playsProperties) {
        playsProperties.forEach(plays -> {
            define(plays.relation().get().identity());
            RoleType roleType = getTypeByScopedLabel(plays.role().labelProperty().get()).asRoleType();
            if (plays.overridden().isPresent()) {
                RoleType overriddenType = getTypeByScopedLabel(plays.overridden().get().labelProperty().get()).asRoleType();
                thingType.setPlays(roleType, overriddenType);
            } else {
                thingType.setPlays(roleType);
            }
        });
    }
}
