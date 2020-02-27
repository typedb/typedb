package grakn.core.graql.reasoner.atom.task.validate;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RelationAtomValidator implements AtomValidator<RelationAtom> {

    @Override
    public Set<String> validateAsRuleHead(RelationAtom atom, Rule rule) {
        //can form a rule head if type is specified, type is not implicit and all relation players are insertable
        return Sets.union(new BasicAtomValidator().validateAsRuleHead(atom, rule), validateRelationPlayers(atom, rule));
    }

    @Override
    public Set<String> validateAsRuleBody(RelationAtom atom, Label ruleLabel) {
        Set<String> errors = new HashSet<>();
        SchemaConcept type = atom.getSchemaConcept();
        if (type != null && !type.isRelationType()) {
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage(ruleLabel, type.label()));
            return errors;
        }

        //check role-type compatibility
        SetMultimap<Variable, Type> varTypeMap = atom.getParentQuery().getVarTypeMap();
        for (Map.Entry<Role, Collection<Variable>> e : atom.getRoleVarMap().asMap().entrySet()) {
            Role role = e.getKey();
            if (!Schema.MetaSchema.isMetaLabel(role.label())) {
                //check whether this role can be played in this relation
                if (type != null && type.asRelationType().roles().noneMatch(r -> r.equals(role))) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage(ruleLabel, role.label(), type.label()));
                }

                //check whether the role player's type allows playing this role
                for (Variable player : e.getValue()) {
                    varTypeMap.get(player).stream()
                            .filter(playerType -> playerType.playing().noneMatch(plays -> plays.equals(role)))
                            .forEach(playerType ->
                                    errors.add(ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage(ruleLabel, playerType.label(), role.label(), type == null ? "" : type.label()))
                            );
                }
            }
        }
        return errors;
    }

    private Set<String> validateRelationPlayers(RelationAtom atom, Rule rule) {
        Set<String> errors = new HashSet<>();
        ConceptManager conceptManager = atom.context().conceptManager();
        atom.getRelationPlayers().forEach(rp -> {
            Statement role = rp.getRole().orElse(null);
            if (role == null) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
            } else {
                String roleLabel = role.getType().orElse(null);
                if (roleLabel == null) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                } else {
                    if (Schema.MetaSchema.isMetaLabel(Label.of(roleLabel))) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE.getMessage(rule.then(), rule.label()));
                    }
                    Role roleType = conceptManager.getRole(roleLabel);
                    if (roleType != null && roleType.isImplicit()) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE.getMessage(rule.then(), rule.label()));
                    }
                }
            }
        });
        return errors;
    }

}
