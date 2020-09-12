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
import grakn.core.query.pattern.Conjunction;
import grakn.core.query.pattern.constraint.TypeConstraint;
import grakn.core.query.pattern.variable.TypeVariable;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MODIFIED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_DEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.SUPERTYPE_TOO_MANY;

public class Definer {

    private static final String TRACE_PREFIX = "definewriter.";

    private final Concepts conceptMgr;
    private final Context.Query context;
    private final Set<TypeVariable> visited;
    private final List<ThingType> defined;
    private final Conjunction<TypeVariable> variables;

    public Definer(Concepts conceptMgr, Conjunction<TypeVariable> variables, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.conceptMgr = conceptMgr;
            this.context = context;
            this.variables = variables;
            this.visited = new HashSet<>();
            this.defined = new LinkedList<>();
        }
    }

    public List<ThingType> write() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "write")) {
            variables.patterns().forEach(variable -> {
                if (!visited.contains(variable)) define(variable);
            });
            return list(defined);
        }
    }

    private Type define(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            assert variable.label().isPresent();
            TypeConstraint.Label labelConstraint = variable.label().get();

            if (labelConstraint.scope().isPresent() && variable.constraints().size() > 1) {
                throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(labelConstraint.scopedLabel()));
            } else if (labelConstraint.scope().isPresent()) return null; // do nothing
            else if (visited.contains(variable)) return conceptMgr.getType(labelConstraint.scopedLabel());

            ThingType type = getThingType(labelConstraint);
            if (variable.sub().size() == 1) {
                type = defineSub(type, variable.sub().iterator().next(), variable);
            } else if (variable.sub().size() > 1) {
                throw GraknException.of(SUPERTYPE_TOO_MANY.message(labelConstraint.label()));
            } else if (variable.valueType().isPresent()) { // && variable.sub().size() == 0
                throw new GraknException(ATTRIBUTE_VALUE_TYPE_MODIFIED.message(
                        variable.valueType().get().valueType().name(), labelConstraint.label()
                ));
            } else if (type == null) {
                throw new GraknException(TYPE_NOT_FOUND.message(labelConstraint.label()));
            }

            if (variable.abstractConstraint().isPresent()) defineAbstract(type);
            if (variable.regex().isPresent())
                defineRegex(type.asAttributeType().asString(), variable.regex().get());
            // TODO: if (variable.when().isPresent()) defineWhen(variable);
            // TODO: if (variable.then().isPresent()) defineThen(variable);

            if (!variable.relates().isEmpty()) defineRelates(type.asRelationType(), variable.relates());
            if (!variable.owns().isEmpty()) defineOwns(type, variable.owns());
            if (!variable.plays().isEmpty()) definePlays(type, variable.plays());

            // if this variable had more constraints beyond being referred to using a label, add to list of defined types
            if (variable.constraints().size() > 1) defined.add(type);
            visited.add(variable);
            return type;
        }
    }

    private ThingType getThingType(TypeConstraint.Label label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthingtype")) {
            Type type;
            if ((type = conceptMgr.getType(label.label())) != null) return type.asThingType();
            else return null;
        }
    }

    private RoleType getRoleType(TypeConstraint.Label label) {
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

    private ThingType defineSub(ThingType thingType, TypeConstraint.Sub subConstraint, TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "definesub")) {
            TypeConstraint.Label labelConstraint = variable.label().get();
            ThingType supertype = define(subConstraint.type()).asThingType();
            if (supertype instanceof EntityType) {
                if (thingType == null) thingType = conceptMgr.putEntityType(labelConstraint.label());
                thingType.asEntityType().setSupertype(supertype.asEntityType());
            } else if (supertype instanceof RelationType) {
                if (thingType == null) thingType = conceptMgr.putRelationType(labelConstraint.label());
                thingType.asRelationType().setSupertype(supertype.asRelationType());
            } else if (supertype instanceof AttributeType) {
                AttributeType.ValueType valueType;
                if (variable.valueType().isPresent()) valueType = variable.valueType().get().valueType();
                else if (!supertype.isRoot()) valueType = supertype.asAttributeType().getValueType();
                else throw new GraknException(ATTRIBUTE_VALUE_TYPE_MISSING.message(labelConstraint.label()));
                if (thingType == null) thingType = conceptMgr.putAttributeType(labelConstraint.label(), valueType);
                thingType.asAttributeType().setSupertype(supertype.asAttributeType());
            } else {
                throw new GraknException(INVALID_DEFINE_SUB.message(labelConstraint.scopedLabel(), supertype.getLabel()));
            }
            return thingType;
        }
    }

    private void defineAbstract(ThingType thingType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineabstract")) {
            thingType.setAbstract();
        }
    }

    private void defineRegex(AttributeType.String attributeType, TypeConstraint.Regex regexConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineregex")) {
            attributeType.setRegex(regexConstraint.regex());
        }
    }

    private void defineRelates(RelationType relationType, Set<TypeConstraint.Relates> relatesConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "definerelates")) {
            relatesConstraints.forEach(relates -> {
                String roleTypeLabel = relates.role().label().get().label();
                if (relates.overridden().isPresent()) {
                    String overriddenTypeLabel = relates.overridden().get().label().get().label();
                    relationType.setRelates(roleTypeLabel, overriddenTypeLabel);
                    visited.add(relates.overridden().get());
                } else {
                    relationType.setRelates(roleTypeLabel);
                }
                visited.add(relates.role());
            });
        }
    }

    private void defineOwns(ThingType thingType, Set<TypeConstraint.Owns> ownsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineowns")) {
            ownsConstraints.forEach(owns -> {
                AttributeType attributeType = define(owns.attribute()).asAttributeType();
                if (owns.overridden().isPresent()) {
                    AttributeType overriddenType = define(owns.overridden().get()).asAttributeType();
                    thingType.setOwns(attributeType, overriddenType, owns.isKey());
                } else {
                    thingType.setOwns(attributeType, owns.isKey());
                }
            });
        }
    }

    private void definePlays(ThingType thingType, Set<TypeConstraint.Plays> playsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "defineplays")) {
            playsConstraints.forEach(plays -> {
                define(plays.relation().get());
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
