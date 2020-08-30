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

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
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
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlDefine;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MODIFIED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_DEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;

public class DefineWriter {

    private static final String TRACE_PREFIX = "definewriter.";

    private final Concepts conceptMgr;
    private final Context.Query context;
    private final Set<Reference> visited;
    private final List<Type> defined;
    private final Map<Reference, TypeVariable> variables;

    public DefineWriter(Concepts conceptMgr, GraqlDefine query, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.conceptMgr = conceptMgr;
            this.context = context;
            this.variables = query.asGraph();
            this.visited = new HashSet<>();
            this.defined = new LinkedList<>();
        }
    }

    public List<Type> write() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "write")) {
            variables.keySet().forEach(reference -> {
                if (!visited.contains(reference)) define(reference);
            });
            return list(defined);
        }
    }

    private Type define(Reference reference) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            TypeVariable variable = variables.get(reference);
            assert variable.label().isPresent();
            TypeProperty.Label labelProperty = variable.label().get();

            if (labelProperty.scope().isPresent() && variable.properties().size() > 1) {
                throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(labelProperty.scopedLabel()));
            } else if (labelProperty.scope().isPresent()) return null; // do nothing
            else if (visited.contains(reference)) return conceptMgr.getType(labelProperty.scopedLabel());

            ThingType type = getThingType(labelProperty);
            if (variable.sub().isPresent()) {
                type = defineSub(type, variable.sub().get(), variable);
            } else if (variable.valueType().isPresent()) {
                throw new GraknException(ATTRIBUTE_VALUE_TYPE_MODIFIED.message(
                        variable.valueType().get().valueType().name(), variable.label().get().label()
                ));
            } else if (type == null) {
                throw new GraknException(TYPE_NOT_FOUND.message(labelProperty.label()));
            }

            if (variable.abstractFlag().isPresent()) defineAbstract(type);
            if (variable.regex().isPresent())
                defineRegex(type.asAttributeType().asString(), variable.regex().get());
            // TODO: if (variable.when().isPresent()) defineWhen(variable);
            // TODO: if (variable.then().isPresent()) defineThen(variable);

            if (!variable.relates().isEmpty()) defineRelates(type.asRelationType(), variable.relates());
            if (!variable.owns().isEmpty()) defineOwns(type, variable.owns());
            if (!variable.plays().isEmpty()) definePlays(type, variable.plays());

            // if this variable had more properties beyond being referred to using a label, add to list of defined types
            if (variable.properties().size() > 1) defined.add(type);
            visited.add(reference);
            return type;
        }
    }

    private ThingType getThingType(TypeProperty.Label label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthingtype")) {
            Type type;
            if ((type = conceptMgr.getType(label.label())) != null) return type.asThingType();
            else return null;
        }
    }

    private RoleType getRoleType(TypeProperty.Label label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getroletype")) {
            // We always assume that Role Types already exist,
            // defined by their Relation Types ahead of time
            assert label.scope().isPresent();
            Type type;
            RoleType roleType;
            if ((type = conceptMgr.getType(label.scope().get())) == null ||
                    (roleType = type.asRelationType().getRelates(label.label())) == null) {
                throw new GraknException(TYPE_NOT_FOUND.message(label.scopedLabel()));
            }
            return roleType;
        }
    }

    private AttributeType.ValueType getValueType(TypeProperty.ValueType valueTypeProperty) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getvaluetype")) {
            return AttributeType.ValueType.of(valueTypeProperty.valueType());
        }
    }

    private ThingType defineSub(ThingType thingType, TypeProperty.Sub subProperty, TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "definesub")) {
            TypeProperty.Label labelProperty = variable.label().get();
            ThingType supertype = define(subProperty.type().reference()).asThingType();
            if (supertype instanceof EntityType) {
                if (thingType == null) thingType = conceptMgr.putEntityType(labelProperty.label());
                thingType.asEntityType().setSupertype(supertype.asEntityType());
            } else if (supertype instanceof RelationType) {
                if (thingType == null) thingType = conceptMgr.putRelationType(labelProperty.label());
                thingType.asRelationType().setSupertype(supertype.asRelationType());
            } else if (supertype instanceof AttributeType) {
                AttributeType.ValueType valueType;
                if (variable.valueType().isPresent()) {
                    valueType = getValueType(variable.valueType().get());
                } else if (!supertype.isRoot()) {
                    valueType = supertype.asAttributeType().getValueType();
                } else {
                    throw new GraknException(ATTRIBUTE_VALUE_TYPE_MISSING.message(labelProperty.label()));
                }
                if (thingType == null) thingType = conceptMgr.putAttributeType(labelProperty.label(), valueType);
                thingType.asAttributeType().setSupertype(supertype.asAttributeType());
            } else {
                throw new GraknException(INVALID_DEFINE_SUB.message(labelProperty.scopedLabel(), supertype.getLabel()));
            }
            return thingType;
        }
    }

    private void defineAbstract(ThingType thingType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineabstract")) {
            thingType.setAbstract();
        }
    }

    private void defineRegex(AttributeType.String attributeType, TypeProperty.Regex regexProperty) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineregex")) {
            attributeType.setRegex(regexProperty.regex());
        }
    }

    private void defineRelates(RelationType relationType, List<TypeProperty.Relates> relatesProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "definerelates")) {
            relatesProperties.forEach(relates -> {
                String roleTypeLabel = relates.role().label().get().label();
                if (relates.overridden().isPresent()) {
                    String overriddenTypeLabel = relates.overridden().get().label().get().label();
                    relationType.setRelates(roleTypeLabel, overriddenTypeLabel);
                    visited.add(relates.overridden().get().reference());
                } else {
                    relationType.setRelates(roleTypeLabel);
                }
                visited.add(relates.role().reference());
            });
        }
    }

    private void defineOwns(ThingType thingType, List<TypeProperty.Owns> ownsProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineowns")) {
            ownsProperties.forEach(owns -> {
                AttributeType attributeType = define(owns.attribute().reference()).asAttributeType();
                if (owns.overridden().isPresent()) {
                    AttributeType overriddenType = define(owns.overridden().get().reference()).asAttributeType();
                    thingType.setOwns(attributeType, overriddenType, owns.isKey());
                } else {
                    thingType.setOwns(attributeType, owns.isKey());
                }
            });
        }
    }

    private void definePlays(ThingType thingType, List<TypeProperty.Plays> playsProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineplays")) {
            playsProperties.forEach(plays -> {
                define(plays.relation().get().reference());
                RoleType roleType = getRoleType(plays.role().label().get()).asRoleType();
                if (plays.overridden().isPresent()) {
                    RoleType overriddenType = getRoleType(plays.overridden().get().label().get()).asRoleType();
                    thingType.setPlays(roleType, overriddenType);
                } else {
                    thingType.setPlays(roleType);
                }
            });
        }
    }
}
