package grakn.core.graql.reasoner.atom.task.validate;

import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class BasicAtomValidator implements AtomValidator<Atom> {

    @Override
    public Set<String> validateAsRuleHead(Atom atom, Rule rule) {
        Set<String> errors = new HashSet<>();
        Set<Atomic> parentAtoms = atom.getParentQuery().getAtoms(Atomic.class).filter(at -> !at.equals(atom)).collect(toSet());
        Set<Variable> varNames = Sets.difference(
                atom.getVarNames(),
                atom.getInnerPredicates().map(Atomic::getVarName).collect(toSet())
        );
        boolean unboundVariables = varNames.stream()
                .anyMatch(var -> parentAtoms.stream().noneMatch(at -> at.getVarNames().contains(var)));
        if (unboundVariables) {
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE.getMessage(rule.then(), rule.label()));
        }

        SchemaConcept schemaConcept = atom.getSchemaConcept();
        if (schemaConcept == null) {
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT.getMessage(rule.then(), rule.label()));
        } else if (schemaConcept.isImplicit()) {
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT.getMessage(rule.then(), rule.label()));
        }
        return errors;
    }


    @Override
    public Set<String> validateAsRuleBody(Atom atom, Label ruleLabel) {
        return new HashSet<>();
    }


}
