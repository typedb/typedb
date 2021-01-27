/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.query.GraqlUndefine;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.RuleRead.RULE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.INVALID_UNDEFINE_RULE_BODY;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_UNDEFINED;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_KEY;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_OVERRIDE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_SUB;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_CONSTRAINT_UNACCEPTED;
import static graql.lang.common.GraqlToken.Constraint.IS;

public class Undefiner {

    private static final String TRACE_PREFIX = "undefiner.";

    private final LogicManager logicMgr;
    private final ConceptManager conceptMgr;
    private final LinkedList<TypeVariable> variables;
    private final Context.Query context;
    private final Set<TypeVariable> undefined;
    private final List<graql.lang.pattern.schema.Rule> rules;

    private Undefiner(ConceptManager conceptMgr, LogicManager logicMgr, Set<TypeVariable> variables,
                      List<graql.lang.pattern.schema.Rule> rules, Context.Query context) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.rules = rules;
        this.context = context;
        this.variables = new LinkedList<>();
        this.undefined = new HashSet<>();

        Set<TypeVariable> sorted = new HashSet<>();
        variables.forEach(variable -> {
            if (!sorted.contains(variable)) sort(variable, sorted);
        });
    }

    public static Undefiner create(ConceptManager conceptMgr, LogicManager logicMgr,
                                   GraqlUndefine query, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            Set<TypeVariable> types = VariableRegistry.createFromTypes(query.variables()).types();
            return new Undefiner(conceptMgr, logicMgr, types, query.rules(), context);
        }
    }

    private void sort(TypeVariable variable, Set<TypeVariable> sorted) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "sort")) {
            if (sorted.contains(variable)) return;
            if (variable.sub().isPresent()) sort(variable.sub().get().type(), sorted);
            this.variables.addFirst(variable);
            sorted.add(variable);
        }
    }

    public void execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            rules.forEach(this::undefine);
            variables.forEach(this::undefine);
        }
    }

    private void undefine(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            assert variable.label().isPresent();
            LabelConstraint labelConstraint = variable.label().get();

            if (labelConstraint.scope().isPresent() && variable.constraints().size() > 1) {
                throw GraknException.of(ROLE_DEFINED_OUTSIDE_OF_RELATION, labelConstraint.scopedLabel());
            } else if (!variable.is().isEmpty()) {
                throw GraknException.of(TYPE_CONSTRAINT_UNACCEPTED, IS);
            } else if (labelConstraint.scope().isPresent()) return; // do nothing
            else if (undefined.contains(variable)) return; // do nothing

            ThingType type = getThingType(labelConstraint);

            if (!variable.plays().isEmpty()) undefinePlays(type, variable.plays());
            if (!variable.owns().isEmpty()) undefineOwns(type, variable.owns());
            if (!variable.relates().isEmpty()) undefineRelates(type.asRelationType(), variable.relates());

            // TODO: if (variable.then().isPresent()) undefineThen(variable);
            // TODO: if (variable.when().isPresent()) undefineWhen(variable);
            if (variable.regex().isPresent()) undefineRegex(type.asAttributeType().asString(), variable.regex().get());
            if (variable.abstractConstraint().isPresent()) undefineAbstract(type);

            if (variable.sub().isPresent()) undefineSub(type, variable.sub().get());
            else if (variable.valueType().isPresent()) {
                throw GraknException.of(ATTRIBUTE_VALUE_TYPE_UNDEFINED,
                                        variable.valueType().get().valueType().name(),
                                        variable.label().get().label());
            }

            undefined.add(variable);
        }
    }

    private ThingType getThingType(LabelConstraint label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "gettype")) {
            ThingType thingType;
            if ((thingType = conceptMgr.getThingType(label.label())) != null) return thingType;
            else return null;
        }
    }

    private RoleType getRoleType(LabelConstraint label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            assert label.scope().isPresent();
            ThingType thingType = conceptMgr.getThingType(label.scope().get());
            if (thingType != null) return thingType.asRelationType().getRelates(label.label());
            return null;
        }
    }

    private void undefineSub(ThingType thingType, SubConstraint subConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_sub")) {
            if (thingType instanceof RoleType) {
                throw GraknException.of(ROLE_DEFINED_OUTSIDE_OF_RELATION, thingType.getLabel());
            }
            ThingType supertype = getThingType(subConstraint.type().label().get());
            if (supertype == null) {
                throw GraknException.of(TYPE_NOT_FOUND, subConstraint.type().label().get());
            } else if (thingType.getSupertypes().noneMatch(t -> t.equals(supertype))) {
                throw GraknException.of(INVALID_UNDEFINE_SUB, thingType.getLabel(), supertype.getLabel());
            }
            if (thingType instanceof RelationType) {
                variables.stream().filter(v -> v.label().isPresent() && v.label().get().scope().isPresent() &&
                        v.label().get().scope().get().equals(thingType.getLabel().name())).forEach(undefined::add);
            }
            thingType.delete();
        }
    }

    private void undefineAbstract(ThingType thingType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_abstract")) {
            thingType.unsetAbstract();
        }
    }

    private void undefineRegex(AttributeType.String attributeType, RegexConstraint regexConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_regex")) {
            if (attributeType.getRegex().pattern().equals(regexConstraint.regex().pattern())) {
                attributeType.unsetRegex();
            }
        }
    }

    private void undefineRelates(RelationType relationType, Set<RelatesConstraint> relatesConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_relates")) {
            relatesConstraints.forEach(relates -> {
                String roleTypeLabel = relates.role().label().get().label();
                if (roleTypeLabel == null) {
                    throw GraknException.of(TYPE_NOT_FOUND, relates.role().label().get().label());
                } else if (relates.overridden().isPresent()) {
                    throw GraknException.of(INVALID_UNDEFINE_RELATES_OVERRIDE,
                                            relates.overridden().get().label().get().label(),
                                            relates.role().label().get());
                } else {
                    relationType.unsetRelates(roleTypeLabel);
                    undefined.add(relates.role());
                }
            });
        }
    }

    private void undefineOwns(ThingType thingType, Set<OwnsConstraint> ownsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_owns")) {
            ownsConstraints.forEach(owns -> {
                Type attributeType = getThingType(owns.attribute().label().get());
                if (attributeType == null && !undefined.contains(owns.attribute())) {
                    throw GraknException.of(TYPE_NOT_FOUND, owns.attribute().label().get().label());
                } else if (owns.overridden().isPresent()) {
                    throw GraknException.of(INVALID_UNDEFINE_OWNS_OVERRIDE,
                                            owns.overridden().get().label().get().label(),
                                            owns.attribute().label().get());
                } else if (owns.isKey()) {
                    throw GraknException.of(INVALID_UNDEFINE_OWNS_KEY,
                                            owns.attribute().label().get(),
                                            owns.attribute().label().get());
                } else if (attributeType != null) {
                    thingType.unsetOwns(attributeType.asAttributeType());
                }
            });
        }
    }

    private void undefinePlays(ThingType thingType, Set<PlaysConstraint> playsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_plays")) {
            playsConstraints.forEach(plays -> {
                Type roleType = getRoleType(plays.role().label().get());
                if (roleType == null && !undefined.contains(plays.role())) {
                    throw GraknException.of(TYPE_NOT_FOUND, plays.role().label().get().label());
                } else if (plays.overridden().isPresent()) {
                    throw GraknException.of(INVALID_UNDEFINE_PLAYS_OVERRIDE,
                                            plays.overridden().get().label().get().label(),
                                            plays.role().label().get());
                } else if (roleType != null) {
                    thingType.unsetPlays(roleType.asRoleType());
                }
            });
        }
    }

    private void undefine(graql.lang.pattern.schema.Rule rule) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine_plays")) {
            if (rule.when() != null || rule.then() != null) {
                throw GraknException.of(INVALID_UNDEFINE_RULE_BODY, rule.label());
            }
            grakn.core.logic.Rule r = logicMgr.getRule(rule.label());
            if (r == null) throw GraknException.of(RULE_NOT_FOUND, rule.label());
            r.delete();
        }
    }
}
