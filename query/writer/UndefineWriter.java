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
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import graql.lang.pattern.property.TypeProperty;
import graql.lang.pattern.variable.Identity;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlUndefine;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_UNDEFINED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_KEY;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_REGEX;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;

public class UndefineWriter {

    private static final String TRACE_PREFIX = "undefinewriter.";
    private final Concepts conceptMgr;
    private final Context.Query context;
    private final GraqlUndefine query;
    private final LinkedList<TypeVariable> variables;
    private final Set<Identity> undefined;
    private final Set<Identity> sorted;

    public UndefineWriter(Concepts conceptMgr, GraqlUndefine query, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.conceptMgr = conceptMgr;
            this.context = context;
            this.query = query;
            this.variables = new LinkedList<>();
            this.undefined = new HashSet<>();
            this.sorted = new HashSet<>();

            query.asGraph().keySet().forEach(identity -> {
                if (!sorted.contains(identity)) sort(identity);
            });
        }
    }

    private void sort(Identity identity) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "sort")) {
            if (this.sorted.contains(identity)) return;
            TypeVariable variable = query.asGraph().get(identity);
            if (variable.sub().isPresent()) sort(variable.sub().get().type().identity());
            this.variables.addFirst(variable);
            this.sorted.add(variable.identity());
        }
    }

    public void write() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "write")) {
            variables.forEach(this::undefine);
        }
    }

    private void undefine(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            assert variable.label().isPresent();
            TypeProperty.Label labelProperty = variable.label().get();

            if (labelProperty.scope().isPresent() && variable.properties().size() > 1) {
                throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(labelProperty.scopedLabel()));
            } else if (labelProperty.scope().isPresent()) return; // do nothing
            else if (undefined.contains(variable.identity())) return; // do nothing

            ThingType type = getType(labelProperty);
            if (type == null) throw new GraknException(TYPE_NOT_FOUND.message(labelProperty.label()));

            if (!variable.plays().isEmpty()) undefinePlays(type, variable.plays());
            if (!variable.owns().isEmpty()) undefineOwns(type, variable.owns());
            if (!variable.relates().isEmpty()) undefineRelates(type.asRelationType(), variable.relates());

            // TODO: if (variable.then().isPresent()) undefineThen(variable);
            // TODO: if (variable.when().isPresent()) undefineWhen(variable);
            if (variable.regex().isPresent())
                undefineRegex(type.asAttributeType().asString(), variable.regex().get());
            if (variable.abstractFlag().isPresent()) undefineAbstract(type);

            if (variable.sub().isPresent()) undefineSub(type, variable.sub().get());
            else if (variable.valueType().isPresent()) {
                throw new GraknException(ATTRIBUTE_VALUE_TYPE_UNDEFINED.message(
                        variable.valueType().get().valueType().name(),
                        variable.label().get().label()
                ));
            }

            undefined.add(variable.identity());
        }
    }

    private ThingType getType(TypeProperty.Label label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "gettype")) {
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

    private void undefineSub(ThingType thingType, TypeProperty.Sub subProperty) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefinesub")) {
            if (thingType instanceof RoleType) {
                throw new GraknException(ROLE_DEFINED_OUTSIDE_OF_RELATION.message(thingType.getLabel()));
            }
            ThingType supertype = getType(subProperty.type().label().get());
            if (supertype == null) {
                throw new GraknException(TYPE_NOT_FOUND.message(subProperty.type().label().get()));
            } else if (thingType.getSupertypes().noneMatch(t -> t.equals(supertype))) {
                throw new GraknException(INVALID_UNDEFINE_SUB.message(thingType.getLabel(), supertype.getLabel()));
            }
            thingType.delete();
        }
    }

    private void undefineAbstract(ThingType thingType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefineabstract")) {
            thingType.unsetAbstract();
        }
    }

    private void undefineRegex(AttributeType.String attributeType, TypeProperty.Regex regexProperty) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefineregex")) {
            if (!attributeType.getRegex().equals(regexProperty.regex())) {
                throw new GraknException(INVALID_UNDEFINE_REGEX.message(attributeType.getLabel(), regexProperty.regex()));
            }
            attributeType.unsetRegex();
        }
    }

    private void undefineRelates(RelationType relationType, List<TypeProperty.Relates> relatesProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefinerelates")) {
            relatesProperties.forEach(relates -> {
                String roleTypeLabel = relates.role().label().get().label();
                if (roleTypeLabel == null) {
                    throw new GraknException(TYPE_NOT_FOUND.message(relates.role().label().get().label()));
                } else if (relates.overridden().isPresent()) {
                    throw new GraknException(INVALID_UNDEFINE_RELATES_OVERRIDE.message(
                            relates.overridden().get().label().get().label(),
                            relates.role().label().get()
                    ));
                } else {
                    relationType.unsetRelates(roleTypeLabel);
                    undefined.add(relates.role().identity());
                }
            });
        }
    }

    private void undefineOwns(ThingType thingType, List<TypeProperty.Owns> ownsProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefineowns")) {
            ownsProperties.forEach(owns -> {
                AttributeType attributeType = getType(owns.attribute().label().get()).asAttributeType();
                if (attributeType == null && !undefined.contains(owns.attribute().identity())) {
                    throw new GraknException(TYPE_NOT_FOUND.message(owns.attribute().label().get().label()));
                } else if (owns.overridden().isPresent()) {
                    throw new GraknException(INVALID_UNDEFINE_OWNS_OVERRIDE.message(
                            owns.overridden().get().label().get().label(),
                            owns.attribute().label().get()
                    ));
                } else if (owns.isKey()) {
                    throw new GraknException(INVALID_UNDEFINE_OWNS_KEY.message(owns.attribute().label().get()));
                } else if (attributeType != null) {
                    thingType.unsetOwns(attributeType);
                }
            });
        }
    }

    private void undefinePlays(ThingType thingType, List<TypeProperty.Plays> playsProperties) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefineplays")) {
            playsProperties.forEach(plays -> {
                RoleType roleType = getRoleType(plays.role().label().get()).asRoleType();
                if (roleType == null && !undefined.contains(plays.role().identity())) {
                    throw new GraknException(TYPE_NOT_FOUND.message(plays.role().label().get().label()));
                } else if (plays.overridden().isPresent()) {
                    throw new GraknException(INVALID_UNDEFINE_PLAYS_OVERRIDE.message(
                            plays.overridden().get().label().get().label(),
                            plays.role().label().get()
                    ));
                } else if (roleType != null) {
                    thingType.unsetPlays(roleType);
                }
            });
        }
    }
}
