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
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import graql.lang.pattern.property.TypeProperty;
import graql.lang.pattern.variable.Identity;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlUndefine;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_UNDEFINED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_KEY;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_REGEX;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_NOT_EXIST;

public class UndefineWriter {

    private final Concepts concepts;
    private final Context.Query context;
    private final Set<Identity> undefined;
    private Map<Identity.Label, TypeVariable> variables;

    public UndefineWriter(Concepts concepts, GraqlUndefine query, Context.Query context) {
        this.concepts = concepts;
        this.context = context;
        this.variables = query.asGraph();
        this.undefined = new HashSet<>();
    }

    public void write() {
        variables.keySet().forEach(this::undefine);
    }

    private void undefine(Identity.Label identity) {
        TypeVariable variable = variables.get(identity);
        assert variable.labelProperty().isPresent();
        TypeProperty.Label labelProperty = variable.labelProperty().get();

        if (labelProperty.scope().isPresent() && variable.properties().size() > 1) {
            throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(labelProperty.scopedLabel()));
        } else if (labelProperty.scope().isPresent()) return; // do nothing
        else if (undefined.contains(identity)) return; // do nothing

        ThingType type = getTypeByLabel(labelProperty);
        if (type == null) throw new GraknException(TYPE_NOT_EXIST.message(labelProperty.label()));

        if (!variable.playsProperty().isEmpty()) undefinePlays(type, variable.playsProperty());
        if (!variable.ownsProperty().isEmpty()) undefineOwns(type, variable.ownsProperty());
        if (!variable.relatesProperty().isEmpty()) undefineRelates(type.asRelationType(), variable.relatesProperty());

        // TODO: if (variable.thenProperty().isPresent()) undefineThen(variable);
        // TODO: if (variable.whenProperty().isPresent()) undefineWhen(variable);
        if (variable.regexProperty().isPresent())
            undefineRegex(type.asAttributeType().asString(), variable.regexProperty().get());
        if (variable.abstractProperty().isPresent()) undefineAbstract(type);

        if (variable.subProperty().isPresent()) undefineSub(type, variable.subProperty().get());
        else if (variable.valueTypeProperty().isPresent()) {
            throw new GraknException(ATTRIBUTE_VALUE_TYPE_UNDEFINED.message(
                    variable.valueTypeProperty().get().valueType().name(),
                    variable.labelProperty().get().label()
            ));
        }

        undefined.add(identity);
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

    private void undefineSub(ThingType thingType, TypeProperty.Sub subProperty) {
        if (thingType instanceof RoleType) {
            throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(thingType.getLabel()));
        }
        ThingType supertype = getTypeByLabel(subProperty.type().labelProperty().get());

        if (supertype == null) {
            throw new GraknException(TYPE_NOT_EXIST.message(subProperty.type().labelProperty().get()));
        } else if (thingType.getSupertypes().noneMatch(t -> t.equals(supertype))) {
            throw new GraknException(INVALID_UNDEFINE_SUB.message(thingType.getLabel(), supertype.getLabel()));
        }

        thingType.delete();
    }

    private void undefineAbstract(ThingType thingType) {
        thingType.unsetAbstract();
    }

    private void undefineRegex(AttributeType.String attributeType, TypeProperty.Regex regexProperty) {
        if (!attributeType.getRegex().equals(regexProperty.regex())) {
            throw new GraknException(INVALID_UNDEFINE_REGEX.message(attributeType.getLabel(), regexProperty.regex()));
        }
        attributeType.unsetRegex();
    }

    private void undefineRelates(RelationType relationType, List<TypeProperty.Relates> relatesProperties) {
        relatesProperties.forEach(relates -> {
            String roleTypeLabel = relates.role().labelProperty().get().label();
            if (relates.overridden().isPresent()) {
                throw new GraknException(INVALID_UNDEFINE_RELATES_OVERRIDE.message(
                        relates.overridden().get().labelProperty().get().label(),
                        relates.role().labelProperty().get()
                ));
            } else {
                relationType.unsetRelates(roleTypeLabel);
            }
        });
    }

    private void undefineOwns(ThingType thingType, List<TypeProperty.Owns> ownsProperties) {
        ownsProperties.forEach(owns -> {
            AttributeType attributeType = getTypeByLabel(owns.attribute().labelProperty().get()).asAttributeType();
            if (owns.overridden().isPresent()) {
                throw new GraknException(INVALID_UNDEFINE_OWNS_OVERRIDE.message(
                        owns.overridden().get().labelProperty().get().label(),
                        owns.attribute().labelProperty().get()
                ));
            } else if (owns.isKey()) {
                throw new GraknException(INVALID_UNDEFINE_OWNS_KEY.message(owns.attribute().labelProperty().get()));
            } else {
                thingType.unsetOwns(attributeType);
            }
        });
    }

    private void undefinePlays(ThingType thingType, List<TypeProperty.Plays> playsProperties) {
        playsProperties.forEach(plays -> {
            RoleType roleType = getTypeByScopedLabel(plays.role().labelProperty().get()).asRoleType();
            if (plays.overridden().isPresent()) {
                throw new GraknException(INVALID_UNDEFINE_PLAYS_OVERRIDE.message(
                        plays.overridden().get().labelProperty().get().label(),
                        plays.role().labelProperty().get()
                ));
            } else {
                thingType.unsetPlays(roleType);
            }
        });
    }
}
