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

package grakn.core.graql.reasoner.atom.task.validate;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Set;

public class AttributeAtomValidator implements AtomValidator<AttributeAtom> {

    private final BasicAtomValidator basicValidator;

    public AttributeAtomValidator(){
        this.basicValidator = new BasicAtomValidator();
    }

    @Override
    public void checkValid(AttributeAtom atom, ReasoningContext ctx) {
        basicValidator.checkValid(atom, ctx);
        SchemaConcept type = atom.getSchemaConcept();
        if (type != null && !type.isAttributeType()) {
            throw GraqlSemanticException.attributeWithNonAttributeType(type.label());
        }
    }

    @Override
    public Set<String> validateAsRuleHead(AttributeAtom atom, Rule rule, ReasoningContext ctx){
        Set<String> errors = basicValidator.validateAsRuleHead(atom, rule, ctx);
        SchemaConcept type = atom.getSchemaConcept();
        Set<ValuePredicate> multiPredicate = atom.getMultiPredicate();

        if ( type == null || multiPredicate.size() > 1){
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_AMBIGUOUS_PREDICATES.getMessage(rule.then(), rule.label()));
        }

        Variable attributeVar = atom.getAttributeVariable();
        if (multiPredicate.isEmpty()){
            boolean predicateBound = atom.getParentQuery().getAtoms(Atom.class)
                    .filter(at -> !at.equals(atom))
                    .anyMatch(at -> at.getVarNames().contains(attributeVar));
            if (!predicateBound) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.then(), rule.label()));
            }

            AttributeType.DataType<Object> dataType = type.asAttributeType().dataType();
            ResolvableQuery body = CacheCasting.ruleCacheCast(ctx.ruleCache()).getRule(rule).getBody();
            ErrorMessage incompatibleValuesMsg = ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_COPYING_INCOMPATIBLE_ATTRIBUTE_VALUES;
            body.getAtoms(AttributeAtom.class)
                    .filter(at -> at.getAttributeVariable().equals(attributeVar))
                    .map(AttributeAtom::getSchemaConcept)
                    .filter(t -> !t.asAttributeType().dataType().equals(dataType))
                    .forEach(t -> errors.add(incompatibleValuesMsg.getMessage(type.label(), rule.label(), t.label())));
        }

        multiPredicate.stream()
                .filter(p -> !p.getPredicate().isValueEquality())
                .forEach( p ->
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_NONSPECIFIC_PREDICATE.getMessage(rule.then(), rule.label()))
                );
        return errors;
    }

    @Override
    public Set<String> validateAsRuleBody(AttributeAtom atom, Label ruleLabel, ReasoningContext ctx) {
        SchemaConcept type = atom.getSchemaConcept();
        Set<String> errors = new HashSet<>();
        if (type == null) return errors;

        if (!type.isAttributeType()){
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage(ruleLabel, type.label()));
            return errors;
        }

        Type ownerType = atom.getParentQuery().getUnambiguousType(atom.getVarName(), false);

        if (ownerType != null
                && ownerType.attributes().noneMatch(rt -> rt.equals(type.asAttributeType()))){
            errors.add(ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage(ruleLabel, type.label(), ownerType.label()));
        }
        return errors;
    }
}
