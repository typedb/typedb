/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.query;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.OwnsConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.PlaysConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RegexConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RelatesConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.SubConstraint;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typeql.lang.query.TypeQLUndefine;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleRead.RULE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.INVALID_UNDEFINE_RULE_BODY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_UNDEFINED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_ANNOTATIONS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_OWNS_OVERRIDE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_PLAYS_OVERRIDE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_RELATES_OVERRIDE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.INVALID_UNDEFINE_SUB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROLE_DEFINED_OUTSIDE_OF_RELATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_CONSTRAINT_UNACCEPTED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.IS;

public class Undefiner {

    private final LogicManager logicMgr;
    private final ConceptManager conceptMgr;
    private final LinkedList<TypeVariable> variables;
    private final Context.Query context;
    private final Set<TypeVariable> undefined;
    private final List<com.vaticle.typeql.lang.pattern.schema.Rule> rules;

    private Undefiner(ConceptManager conceptMgr, LogicManager logicMgr, Set<TypeVariable> variables,
                      List<com.vaticle.typeql.lang.pattern.schema.Rule> rules, Context.Query context) {
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
                                   TypeQLUndefine query, Context.Query context) {
        Set<TypeVariable> types = VariableRegistry.createFromTypes(query.statements()).types();
        iterate(types).flatMap(t -> iterate(t.constraints()))
                .filter(c -> c.isLabel() && c.asLabel().properLabel().scope().isPresent())
                .forEachRemaining(c -> conceptMgr.validateNotRoleTypeAlias(c.asLabel().properLabel()));
        return new Undefiner(conceptMgr, logicMgr, types, query.rules(), context);
    }

    private void sort(TypeVariable variable, Set<TypeVariable> sorted) {
        if (sorted.contains(variable)) return;
        if (variable.sub().isPresent()) sort(variable.sub().get().type(), sorted);
        this.variables.addFirst(variable);
        sorted.add(variable);
    }

    public void execute() {
        rules.forEach(this::undefine);
        variables.forEach(this::undefine);
    }

    private void undefine(TypeVariable variable) {
        assert variable.label().isPresent();
        LabelConstraint labelConstraint = variable.label().get();

        if (labelConstraint.scope().isPresent() && variable.constraints().size() > 1) {
            throw TypeDBException.of(ROLE_DEFINED_OUTSIDE_OF_RELATION, labelConstraint.scopedLabel());
        } else if (!variable.is().isEmpty()) {
            throw TypeDBException.of(TYPE_CONSTRAINT_UNACCEPTED, IS);
        } else if (labelConstraint.scope().isPresent()) return; // do nothing
        else if (undefined.contains(variable)) return; // do nothing

        ThingType type = getThingType(labelConstraint);
        if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, labelConstraint.scopedLabel());

        if (!variable.plays().isEmpty()) undefinePlays(type, variable.plays());
        if (!variable.owns().isEmpty()) undefineOwns(type, variable.owns());
        if (!variable.relates().isEmpty()) undefineRelates(type.asRelationType(), variable.relates());

        if (variable.regex().isPresent()) undefineRegex(type.asAttributeType().asString(), variable.regex().get());
        if (variable.abstractConstraint().isPresent()) undefineAbstract(type);

        if (variable.sub().isPresent()) undefineSub(type, variable.sub().get());
        else if (variable.valueType().isPresent()) {
            throw TypeDBException.of(ATTRIBUTE_VALUE_TYPE_UNDEFINED,
                    variable.valueType().get().valueType().name(),
                    variable.label().get().label());
        }

        undefined.add(variable);
    }

    private ThingType getThingType(LabelConstraint label) {
        return conceptMgr.getThingType(label.label());
    }

    private RoleType getRoleType(LabelConstraint label) {
        assert label.scope().isPresent();
        ThingType thingType = conceptMgr.getThingType(label.scope().get());
        if (thingType != null) return thingType.asRelationType().getRelates(label.label());
        return null;
    }

    private void undefineSub(ThingType thingType, SubConstraint subConstraint) {
        if (thingType.isRoleType()) {
            throw TypeDBException.of(ROLE_DEFINED_OUTSIDE_OF_RELATION, thingType.getLabel());
        }
        ThingType supertype = getThingType(subConstraint.type().label().get());
        if (supertype == null) {
            throw TypeDBException.of(TYPE_NOT_FOUND, subConstraint.type().label().get().scopedLabel());
        } else if (thingType.getSupertypes().noneMatch(t -> t.equals(supertype))) {
            throw TypeDBException.of(INVALID_UNDEFINE_SUB, thingType.getLabel(), supertype.getLabel());
        }
        if (thingType.isRelationType()) {
            variables.stream().filter(v -> v.label().isPresent() && v.label().get().scope().isPresent() &&
                    v.label().get().scope().get().equals(thingType.getLabel().name())).forEach(undefined::add);
        }
        thingType.delete();
    }

    private void undefineAbstract(ThingType thingType) {
        thingType.unsetAbstract();
    }

    private void undefineRegex(AttributeType.String attributeType, RegexConstraint regexConstraint) {
        if (attributeType.getRegex().pattern().equals(regexConstraint.regex().pattern())) {
            attributeType.unsetRegex();
        }
    }

    private void undefineRelates(RelationType relationType, Set<RelatesConstraint> relatesConstraints) {
        relatesConstraints.forEach(relates -> {
            String roleTypeLabel = relates.role().label().get().label();
            if (roleTypeLabel == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, relates.role().label().get().label());
            } else if (relates.overridden().isPresent()) {
                throw TypeDBException.of(INVALID_UNDEFINE_RELATES_OVERRIDE,
                        relates.overridden().get().label().get().label(),
                        relates.role().label().get());
            } else {
                relationType.unsetRelates(roleTypeLabel);
                undefined.add(relates.role());
            }
        });
    }

    private void undefineOwns(ThingType thingType, Set<OwnsConstraint> ownsConstraints) {
        ownsConstraints.forEach(owns -> {
            Type attributeType = getThingType(owns.attribute().label().get());
            if (attributeType == null && !undefined.contains(owns.attribute())) {
                throw TypeDBException.of(TYPE_NOT_FOUND, owns.attribute().label().get().label());
            } else if (owns.overridden().isPresent()) {
                throw TypeDBException.of(INVALID_UNDEFINE_OWNS_OVERRIDE,
                        owns.overridden().get().label().get().label(),
                        owns.attribute().label().get());
            } else if (!owns.annotations().isEmpty()) throw TypeDBException.of(INVALID_UNDEFINE_ANNOTATIONS, owns);
            else if (attributeType != null) thingType.unsetOwns(attributeType.asAttributeType());
        });
    }

    private void undefinePlays(ThingType thingType, Set<PlaysConstraint> playsConstraints) {
        playsConstraints.forEach(plays -> {
            Type roleType = getRoleType(plays.role().label().get());
            if (roleType == null && !undefined.contains(plays.role())) {
                throw TypeDBException.of(TYPE_NOT_FOUND, plays.role().label().get().label());
            } else if (plays.overridden().isPresent()) {
                throw TypeDBException.of(INVALID_UNDEFINE_PLAYS_OVERRIDE,
                        plays.overridden().get().label().get().label(),
                        plays.role().label().get());
            } else if (roleType != null) {
                thingType.unsetPlays(roleType.asRoleType());
            }
        });
    }

    private void undefine(com.vaticle.typeql.lang.pattern.schema.Rule rule) {
        if (rule.when() != null || rule.then() != null) {
            throw TypeDBException.of(INVALID_UNDEFINE_RULE_BODY, rule.label());
        }
        com.vaticle.typedb.core.logic.Rule r = logicMgr.getRule(rule.label());
        if (r == null) throw TypeDBException.of(RULE_NOT_FOUND, rule.label());
        logicMgr.deleteAndInvalidateRule(r);
    }
}
