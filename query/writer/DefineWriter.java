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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MODIFIED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_UNDEFINED;

public class DefineWriter {

    private final Concepts concepts;
    private final Context.Query context;
    private final Set<Identity> visited;
    private final List<Type> output;
    private Map<Identity.Label, TypeVariable> variables;

    public DefineWriter(Concepts concepts, GraqlDefine query, Context.Query context) {
        this.concepts = concepts;
        this.context = context;
        this.variables = query.asGraph();
        this.visited = new HashSet<>();
        this.output = new LinkedList<>();
    }

    public List<Type> write() {
        variables.keySet().forEach(identity -> {
            if (!visited.contains(identity)) define(identity);
        });
        return list(output);
    }

    private Type define(Identity.Label identity) {
        TypeVariable variable = variables.get(identity);
        assert variable.labelProperty().isPresent();
        TypeProperty.Label labelProperty = variable.labelProperty().get();
        if (visited.contains(identity)) return concepts.getType(labelProperty.scopedLabel());

        ThingType type;
        if (labelProperty.scope().isPresent()) return defineLabelScoped(variable);
        else type = defineLabel(variable);
        if (variable.subProperty().isPresent()) type = defineSub(variable);

        if (type == null) throw new GraknException(TYPE_UNDEFINED.message(labelProperty.label()));

        if (variable.abstractProperty().isPresent()) defineAbstract(variable);
        if (variable.valueTypeProperty().isPresent()) defineValueType(variable);
        if (variable.regexProperty().isPresent()) defineRegex(variable);
        // TODO: if (variable.thenProperty().isPresent()) defineThen(variable);
        // TODO: if (variable.whenProperty().isPresent()) defineWhen(variable);

        if (!variable.relatesProperty().isEmpty()) defineRelates(variable);
        if (!variable.ownsProperty().isEmpty()) defineOwns(variable);
        if (!variable.playsProperty().isEmpty()) definePlays(variable);

        if (variable.properties().size() > 1) output.add(type);
        visited.add(identity);
        return type;
    }

    private void defineRegex(TypeVariable variable) {
        assert variable.regexProperty().isPresent();
        Type type = concepts.getType(variable.labelProperty().get().label());
        type.asAttributeType().asString().setRegex(variable.regexProperty().get().regex());
    }

    private ThingType defineLabel(TypeVariable variable) {
        assert variable.labelProperty().isPresent();
        assert !variable.labelProperty().get().scope().isPresent();

        Type type;
        if ((type = concepts.getType(variable.labelProperty().get().label())) != null) return type.asThingType();
        else return null;
    }

    private RoleType defineLabelScoped(TypeVariable variable) {
        assert variable.labelProperty().isPresent();
        assert variable.labelProperty().get().scope().isPresent();
        assert variable.properties().size() == 1;

        // We always assume that Role Types already exist, defined by their Relation Types ahead of time
        TypeProperty.Label label = variable.labelProperty().get();
        Type type;
        RoleType roleType;
        if ((type = concepts.getType(label.scope().get())) == null ||
                (roleType = type.asRelationType().getRelates(label.label())) == null) {
            throw new GraknException(TYPE_UNDEFINED.message(label.scopedLabel()));
        }
        return roleType;
    }

    private ThingType defineSub(TypeVariable variable) {
        assert variable.subProperty().isPresent();
        ThingType supertype = define(variable.subProperty().get().type().identity()).asThingType();
        String label = variable.labelProperty().get().label();
        ThingType thingType;
        if (supertype instanceof EntityType) {
            thingType = concepts.putEntityType(label);
            thingType.asEntityType().setSupertype(supertype.asEntityType());
        } else if (supertype instanceof RelationType) {
            thingType = concepts.putRelationType(label);
            thingType.asRelationType().setSupertype(supertype.asRelationType());
        } else if (supertype instanceof AttributeType) {
            AttributeType.ValueType valueType = defineValueType(variable);
            thingType = concepts.putAttributeType(label, valueType);
            thingType.asAttributeType().setSupertype(supertype.asAttributeType());
        } else {
            throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(label));
        }
        return thingType;
    }

    private void defineAbstract(TypeVariable variable) {
        assert variable.abstractProperty().isPresent();
        concepts.getType(variable.labelProperty().get().label()).asThingType().setAbstract();
    }

    private AttributeType.ValueType defineValueType(TypeVariable variable) {
        assert variable.valueTypeProperty().isPresent();
        if (variable.subProperty().isPresent()) {
            return variable.valueTypeProperty()
                    .map(TypeProperty.ValueType::valueType)
                    .map(AttributeType.ValueType::of)
                    .orElse(null);
        } else {
            throw new GraknException(ATTRIBUTE_VALUE_TYPE_MODIFIED.message(
                    variable.valueTypeProperty().get().valueType().name(),
                    variable.labelProperty().get().label()
            ));
        }
    }

    private void defineRelates(TypeVariable variable) {
        RelationType relationType = concepts.getRelationType(variable.labelProperty().get().label());
        variable.relatesProperty().forEach(relates -> {
            String roleTypeLabel = relates.role().labelProperty().get().label();
            if (relates.overridden().isPresent()) {
                String overriddenTypeLabel = relates.overridden().map(o -> o.labelProperty().get().label()).get();
                relationType.setRelates(roleTypeLabel, overriddenTypeLabel);
            } else {
                relationType.setRelates(roleTypeLabel);
            }
        });
    }

    private void defineOwns(TypeVariable variable) {
        ThingType thingType = concepts.getType(variable.labelProperty().get().label()).asThingType();
        variable.ownsProperty().forEach(owns -> {
            AttributeType attributeType = define(owns.attribute().identity()).asAttributeType();
            if (owns.overridden().isPresent()) {
                AttributeType overriddenType = define(owns.overridden().get().identity()).asAttributeType();
                thingType.setOwns(attributeType, overriddenType, owns.isKey());
            } else {
                thingType.setOwns(attributeType, owns.isKey());
            }
        });
    }

    private void definePlays(TypeVariable variable) {
        ThingType thingType = concepts.getType(variable.labelProperty().get().label()).asThingType();
        variable.playsProperty().forEach(plays -> {
            define(plays.relation().get().identity());
            RoleType roleType = define(plays.role().identity()).asRoleType();
            if (plays.overridden().isPresent()) {
                RoleType overriddenType = define(plays.overridden().get().identity()).asRoleType();
                thingType.setPlays(roleType, overriddenType);
            } else {
                thingType.setPlays(roleType);
            }
        });
    }
}
