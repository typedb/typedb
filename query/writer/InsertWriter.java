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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.concept.Concepts;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.property.ThingProperty;
import graql.lang.pattern.property.ValueOperation;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.Identity;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlInsert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Bytes.hexStringToBytes;
import static grakn.core.common.exception.ErrorMessage.ThingRead.THING_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_IID_CONFLICT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_PROPERTY_TYPE_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_PROPERTY_UNACCEPTED;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public class InsertWriter {

    private final Concepts conceptMgr;
    private final Context.Query context;
    private final ConceptMap existing;
    private final Map<Identity, Thing> inserted;
    private Map<Identity, BoundVariable<?>> variables;

    public InsertWriter(Concepts conceptMgr, GraqlInsert query, Context.Query context) {
        this(conceptMgr, query, context, new ConceptMap());
    }

    public InsertWriter(Concepts conceptMgr, GraqlInsert query, Context.Query context, ConceptMap existing) {
        this.conceptMgr = conceptMgr;
        this.context = context;
        this.existing = existing;
        this.variables = query.asGraph();
        this.inserted = new HashMap<>();
    }

    public ConceptMap write() {
        this.variables.forEach((identity, boundVariable) -> {
            if (boundVariable.isThing()) insert(identity);
        });

        return new ConceptMap(inserted);
    }

    private Thing insert(Identity identity) {
        if (inserted.containsKey(identity)) return inserted.get(identity);
        else if (existing.contains(identity)) return existing.get(identity).asThing();
        ThingVariable<?> variable = variables.get(identity).asThing();
        Thing thing;

        if (variable.iidProperty().isPresent() && variable.isaProperty().isPresent()) {
            throw new GraknException(THING_ISA_IID_CONFLICT.message(
                    variable.iidProperty().get(),
                    variable.isaProperty().get().type().labelProperty().get().label()
            ));
        } else if (variable.neqProperty().isPresent()) {
            throw new GraknException(THING_PROPERTY_UNACCEPTED.message(GraqlToken.Comparator.NEQ));
        }
        else if (variable.iidProperty().isPresent()) thing = getThingByIID(variable.iidProperty().get());
        else if (variable.isaProperty().isPresent()) thing = insertIsa(variable.isaProperty().get(), variable);
        else throw new GraknException(THING_ISA_MISSING.message(identity.toString()));

        if (!variable.hasProperties().isEmpty()) insertHas(thing, variable.hasProperties());

        inserted.put(identity, thing);
        return thing;
    }

    private void insertHas(Thing thing, List<ThingProperty.Has> hasProperties) {
        hasProperties.forEach(has -> {
            Attribute attribute = insert(has.variable().identity()).asAttribute();
            thing.setHas(attribute);
        });
    }

    private Thing getThingByIID(ThingProperty.IID iidProperty) {
        Thing thing = conceptMgr.getThing(hexStringToBytes(iidProperty.iid()));
        if (thing == null) throw new GraknException(THING_NOT_FOUND.message(iidProperty.iid()));
        else return thing;
    }

    private ThingType getThingTypeByVariable(TypeVariable variable) {
        if (variable.isLabelled()) {
            assert variable.labelProperty().isPresent();
            Type type = conceptMgr.getType(variable.labelProperty().get().label());
            if (type == null) throw new GraknException(TYPE_NOT_FOUND.message(variable.labelProperty().get().label()));
            else return type.asThingType();
        } else {
            throw new GraknException(THING_PROPERTY_TYPE_VARIABLE.message(variable.identity()));
        }
    }

    private RoleType getRoleTypeByVariable(TypeVariable variable) {
        if (variable.isLabelled()) {
            assert variable.labelProperty().isPresent();
            RelationType relationType; RoleType roleType;
            if ((relationType = conceptMgr.getRelationType(variable.labelProperty().get().scope().get())) != null &&
                    (roleType = relationType.getRelates(variable.labelProperty().get().label())) != null) {
                return roleType;
            } else {
                throw new GraknException(TYPE_NOT_FOUND.message(variable.labelProperty().get().scopedLabel()));
            }
        } else {
            throw new GraknException(THING_PROPERTY_TYPE_VARIABLE.message(variable.identity()));
        }
    }

    private Thing insertIsa(ThingProperty.Isa isaProperty, ThingVariable<?> variable) {
        ThingType thingType = getThingTypeByVariable(isaProperty.type());

        if (thingType instanceof EntityType) {
            return insertEntity(thingType.asEntityType());
        } else if (thingType instanceof AttributeType) {
            return insertAttribute(thingType.asAttributeType(), variable);
        } else if (thingType instanceof RelationType) {
            return insertRelation(thingType.asRelationType(), variable);
        } else {
            assert false;
            return null;
        }
    }

    private Entity insertEntity(EntityType entityType) {
        return entityType.create();
    }

    private Attribute insertAttribute(AttributeType attributeType, ThingVariable<?> variable) {
        ThingProperty.Value<?> valueProperty;
        if (variable.valueProperty().isPresent() && (valueProperty = variable.valueProperty().get()).operation().isAssignment()) {
            ValueOperation.Assignment valueAssignment = valueProperty.operation().asAssignment();
            switch (attributeType.getValueType()) {
                case LONG:
                    return attributeType.asLong().put(valueAssignment.asLong().value());
                case DOUBLE:
                    return attributeType.asDouble().put(valueAssignment.asDouble().value());
                case BOOLEAN:
                    return attributeType.asBoolean().put(valueAssignment.asBoolean().value());
                case STRING:
                    return attributeType.asString().put(valueAssignment.asString().value());
                case DATETIME:
                    return attributeType.asDateTime().put(valueAssignment.asDateTime().value());
                default:
                    assert false;
                    return null;
            }
        } else {
            throw new GraknException(ATTRIBUTE_VALUE_MISSING.message(variable.identity(), attributeType.getLabel()));
        }
    }

    private Relation insertRelation(RelationType relationType, ThingVariable<?> variable) {
        if (variable.relationProperty().isPresent()) {
            Relation relation = relationType.create();
            variable.relationProperty().get().players().forEach(rolePlayer -> {
                RoleType roleType;
                Thing player = insert(rolePlayer.player().identity());
                Set<RoleType> inferred;
                if (rolePlayer.roleType().isPresent()) {
                    roleType = getRoleTypeByVariable(rolePlayer.roleType().get());
                } else if ((inferred = player.getType().getPlays()
                        .filter(rt -> rt.getRelation().equals(relationType))
                        .collect(toSet())).size() == 1) {
                    roleType = inferred.iterator().next();
                } else if (inferred.size() > 1) {
                    throw new GraknException(ROLE_TYPE_AMBIGUOUS.message(rolePlayer.player().identity()));
                } else {
                    throw new GraknException(ROLE_TYPE_MISSING.message(rolePlayer.player().identity()));
                }

                relation.addPlayer(roleType, player);
            });

            return relation;
        } else {
            throw new GraknException(ErrorMessage.ThingWrite.RELATION_PROPERTY_MISSING.message(variable.identity()));
        }
    }
}
