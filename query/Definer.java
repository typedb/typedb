/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.ThreadTrace;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.AttributeType.ValueType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.OwnsConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.PlaysConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RegexConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RelatesConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.SubConstraint;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typeql.lang.query.TypeQLDefine;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_SCOPE_IS_NOT_RELATION_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_DEFINED_NOT_ON_ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MODIFIED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.CYCLIC_TYPE_HIERARCHY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_DEFINE_SUB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_CONSTRAINT_UNACCEPTED;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.IS;

public class Definer {

    private static final String TRACE_PREFIX = "definer.";

    private final LogicManager logicMgr;
    private final ConceptManager conceptMgr;
    private final Set<TypeVariable> variables;
    private final List<com.vaticle.typeql.lang.pattern.schema.Rule> rules;
    private final Context.Query context;
    private final Set<TypeVariable> defined;

    private Definer(ConceptManager conceptMgr, LogicManager logicMgr, Set<TypeVariable> variables,
                    List<com.vaticle.typeql.lang.pattern.schema.Rule> rules, Context.Query context) {
        this.logicMgr = logicMgr;
        this.conceptMgr = conceptMgr;
        this.variables = variables;
        this.rules = rules;
        this.context = context;
        this.defined = new HashSet<>();
    }

    public static Definer create(ConceptManager conceptMgr, LogicManager logicMgr,
                                 TypeQLDefine query, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            Set<TypeVariable> types = VariableRegistry.createFromTypes(query.variables()).types();
            return new Definer(conceptMgr, logicMgr, types, query.rules(), context);
        }
    }

    public void execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            validateTypeHierarchyIsNotCyclic(variables);
            variables.forEach(variable -> {
                if (!defined.contains(variable)) define(variable);
            });
            rules.forEach(this::define);
        }
    }

    private ThingType define(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            assert variable.label().isPresent();
            LabelConstraint labelConstraint = variable.label().get();

            if (labelConstraint.scope().isPresent() && variable.constraints().size() > 1) {
                throw TypeDBException.of(ROLE_DEFINED_OUTSIDE_OF_RELATION, labelConstraint.scopedLabel());
            } else if (!variable.is().isEmpty()) {
                throw TypeDBException.of(TYPE_CONSTRAINT_UNACCEPTED, IS);
            } else if (labelConstraint.scope().isPresent()) return null; // do nothing
            else if (defined.contains(variable)) return conceptMgr.getThingType(labelConstraint.scopedLabel());

            ThingType type = getThingType(labelConstraint);
            if (variable.sub().isPresent()) {
                type = defineSub(type, variable.sub().get(), variable);
            } else if (variable.valueType().isPresent()) { // && variable.sub().size() == 0
                String valueType = variable.valueType().get().valueType().name();
                throw TypeDBException.of(ATTRIBUTE_VALUE_TYPE_MODIFIED, valueType, labelConstraint.label());
            } else if (type == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, labelConstraint.label());
            }

            if (variable.valueType().isPresent() && !(type.isAttributeType())) {
                throw TypeDBException.of(ATTRIBUTE_VALUE_TYPE_DEFINED_NOT_ON_ATTRIBUTE_TYPE, labelConstraint.label());
            }

            defined.add(variable);

            if (variable.abstractConstraint().isPresent()) defineAbstract(type);
            if (variable.regex().isPresent()) defineRegex(type.asAttributeType().asString(), variable.regex().get());

            if (!variable.relates().isEmpty()) defineRelates(type.asRelationType(), variable.relates());
            if (!variable.owns().isEmpty()) defineOwns(type, variable.owns());
            if (!variable.plays().isEmpty()) definePlays(type, variable.plays());

            return type;
        }
    }

    private void validateTypeHierarchyIsNotCyclic(Set<TypeVariable> variables) {
        Set<TypeVariable> visited = new HashSet<>();
        for (TypeVariable variable : variables) {
            if (visited.contains(variable)) continue;
            assert variable.label().isPresent();
            LinkedHashSet<String> hierarchy = new LinkedHashSet<>();
            hierarchy.add(variable.label().get().scopedLabel());
            visited.add(variable);
            while (variable.sub().isPresent()) {
                variable = variable.sub().get().type();
                assert variable.label().isPresent();
                if (!hierarchy.add(variable.label().get().scopedLabel())) {
                    throw TypeDBException.of(CYCLIC_TYPE_HIERARCHY, hierarchy);
                }
            }
        }
    }

    private ThingType getThingType(LabelConstraint label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_thing_type")) {
            ThingType thingType;
            if ((thingType = conceptMgr.getThingType(label.label())) != null) return thingType;
            else return null;
        }
    }

    private RoleType getRoleType(LabelConstraint label) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            // We always assume that Role Types already exist,
            // defined by their Relation Types ahead of time
            assert label.scope().isPresent();
            ThingType thingType;
            RoleType roleType;
            if ((thingType = conceptMgr.getThingType(label.scope().get())) == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, label.scope().get());
            } else if (!thingType.isRelationType()) {
                throw TypeDBException.of(ROLE_TYPE_SCOPE_IS_NOT_RELATION_TYPE, label.scopedLabel(), label.scope().get());
            } else if ((roleType = thingType.asRelationType().getRelates(label.label())) == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, label.scopedLabel());
            }
            return roleType;
        }
    }

    private ThingType defineSub(ThingType thingType, SubConstraint subConstraint, TypeVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_sub")) {
            LabelConstraint labelConstraint = var.label().get();
            ThingType supertype = define(subConstraint.type()).asThingType();
            if (supertype.isEntityType()) {
                if (thingType == null) thingType = conceptMgr.putEntityType(labelConstraint.label());
                thingType.asEntityType().setSupertype(supertype.asEntityType());
            } else if (supertype.isRelationType()) {
                if (thingType == null) thingType = conceptMgr.putRelationType(labelConstraint.label());
                thingType.asRelationType().setSupertype(supertype.asRelationType());
            } else if (supertype.isAttributeType()) {
                ValueType valueType;
                if (var.valueType().isPresent()) valueType = ValueType.of(var.valueType().get().valueType());
                else if (!supertype.isRoot()) valueType = supertype.asAttributeType().getValueType();
                else throw TypeDBException.of(ATTRIBUTE_VALUE_TYPE_MISSING, labelConstraint.label());
                if (thingType == null) thingType = conceptMgr.putAttributeType(labelConstraint.label(), valueType);
                else if (!valueType.equals(thingType.asAttributeType().getValueType())) {
                    throw TypeDBException.of(ATTRIBUTE_VALUE_TYPE_MODIFIED, valueType, labelConstraint.label());
                }
                thingType.asAttributeType().setSupertype(supertype.asAttributeType());
            } else {
                throw TypeDBException.of(INVALID_DEFINE_SUB, labelConstraint.scopedLabel(), supertype.getLabel());
            }
            return thingType;
        }
    }

    private void defineAbstract(ThingType thingType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_abstract")) {
            thingType.setAbstract();
        }
    }

    private void defineRegex(AttributeType.String attributeType, RegexConstraint regexConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_regex")) {
            attributeType.setRegex(regexConstraint.regex());
        }
    }

    private void defineRelates(RelationType relationType, Set<RelatesConstraint> relatesConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_relates")) {
            relatesConstraints.forEach(relates -> {
                String roleTypeLabel = relates.role().label().get().label();
                if (relates.overridden().isPresent()) {
                    String overriddenTypeLabel = relates.overridden().get().label().get().label();
                    relationType.setRelates(roleTypeLabel, overriddenTypeLabel);
                    defined.add(relates.overridden().get());
                } else {
                    relationType.setRelates(roleTypeLabel);
                }
                defined.add(relates.role());
            });
        }
    }

    private void defineOwns(ThingType thingType, Set<OwnsConstraint> ownsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_owns")) {
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

    private void definePlays(ThingType thingType, Set<PlaysConstraint> playsConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define_plays")) {
            playsConstraints.forEach(plays -> {
                define(plays.relation().get());
                LabelConstraint roleTypeLabel = plays.role().label().get();
                RoleType roleType = getRoleType(roleTypeLabel).asRoleType();
                if (plays.overridden().isPresent()) {
                    String overriddenLabelName = plays.overridden().get().label().get().properLabel().name();
                    Optional<? extends RoleType> overriddenType = roleType.getSupertypes()
                            .filter(rt -> rt.getLabel().name().equals(overriddenLabelName)).first();
                    if (overriddenType.isPresent()) thingType.setPlays(roleType, overriddenType.get());
                    else throw TypeDBException.of(OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE,
                            thingType.getLabel(), roleTypeLabel.scopedLabel(), overriddenLabelName);
                } else thingType.setPlays(roleType);
            });
        }
    }

    private void define(com.vaticle.typeql.lang.pattern.schema.Rule rule) {
        logicMgr.putRule(rule.label(), rule.when(), rule.then());
    }
}
