package grakn.core.graql.reasoner.atom.task.validate;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.Binary;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Set;

public class AttributeAtomValidator implements AtomValidator<AttributeAtom> {

    private final RuleCache ruleCache;

    public AttributeAtomValidator(RuleCache ruleCache){
        this.ruleCache = ruleCache;
    }

    @Override
    public Set<String> validateAsRuleHead(AttributeAtom atom, Rule rule){
        Set<String> errors = new BasicAtomValidator().validateAsRuleHead(atom, rule);
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
            ResolvableQuery body = CacheCasting.ruleCacheCast(ruleCache).getRule(rule).getBody();
            ErrorMessage incompatibleValuesMsg = ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_COPYING_INCOMPATIBLE_ATTRIBUTE_VALUES;
            body.getAtoms(AttributeAtom.class)
                    .filter(at -> at.getAttributeVariable().equals(attributeVar))
                    .map(Binary::getSchemaConcept)
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
    public Set<String> validateAsRuleBody(AttributeAtom atom, Label ruleLabel) {
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
