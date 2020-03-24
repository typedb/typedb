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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.Streams;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.NeqIdPredicate;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import graql.lang.pattern.Conjunction;
import graql.lang.property.RelationProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RelationAtomValidator implements AtomValidator<RelationAtom> {

    private final BasicAtomValidator basicValidator;

    public RelationAtomValidator() {
        this.basicValidator = new BasicAtomValidator();
    }

    @Override
    public void checkValid(RelationAtom atom, ReasoningContext ctx) {
        basicValidator.checkValid(atom, ctx);
        SchemaConcept type = atom.getSchemaConcept();
        if (type != null && !type.isRelationType()) {
            throw GraqlSemanticException.relationWithNonRelationType(type.label());
        }
        checkPattern(atom, ctx);
    }

    @Override
    public Set<String> validateAsRuleHead(RelationAtom atom, Rule rule, ReasoningContext ctx) {
        //can form a rule head if type is specified, type is not implicit and all relation players are insertable
        Set<String> errors = Sets.union(
                basicValidator.validateAsRuleHead(atom, rule, ctx),
                validateRelationPlayers(atom, rule, ctx));
        return Sets.union(errors, validateDuplicateInferredRolesMustBeDifferentConcepts(atom, rule, ctx));
    }

    @Override
    public Set<String> validateAsRuleBody(RelationAtom atom, Label ruleLabel, ReasoningContext ctx) {
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

    private void checkPattern(RelationAtom atom, ReasoningContext ctx) {
        ConceptManager conceptManager = ctx.conceptManager();
        atom.getPattern().getProperties(RelationProperty.class)
                .flatMap(p -> p.relationPlayers().stream())
                .map(RelationProperty.RolePlayer::getRole).flatMap(Streams::optionalToStream)
                .map(Statement::getType).flatMap(Streams::optionalToStream)
                .map(Label::of)
                .forEach(roleId -> {
                    SchemaConcept schemaConcept = conceptManager.getSchemaConcept(roleId);
                    if (schemaConcept == null || !schemaConcept.isRole()) {
                        throw GraqlSemanticException.invalidRoleLabel(roleId);
                    }
                });
    }

    private Set<String> validateRelationPlayers(RelationAtom atom, Rule rule, ReasoningContext ctx) {
        Set<String> errors = new HashSet<>();
        ConceptManager conceptManager = ctx.conceptManager();

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

                    // take the role player variable, perform type inference and check if any of the possible types can play
                    // the given roles
                    IsaAtom isaAtom = IsaAtom.create(rp.getPlayer().var(), new Variable(), null, false, atom.getParentQuery(), ctx);
                    ImmutableList<Type> possibleTypesOfRolePlayer = isaAtom.getPossibleTypes();
                    boolean anyCanPlayRequiredRole = possibleTypesOfRolePlayer.stream().anyMatch(type -> type.playing().anyMatch(rolePlayed -> rolePlayed.equals(roleType)));
                    if (!anyCanPlayRequiredRole) {
                        errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ROLE_CANNOT_BE_PLAYED.getMessage(rule.label(), rp.getPlayer().var(), roleLabel));
                    }
                }
            }
        });
        return errors;
    }


    /**
     * We have an over-strict defensive check we enforce for now, to ensure that a rule will not throw at runtime
     * Requiring a '!=' between variables that may result in an inferred relation with duplicate role player edges.
     * For example:
     */
    private Set<String> validateDuplicateInferredRolesMustBeDifferentConcepts(RelationAtom headAtom, Rule rule, ReasoningContext ctx) {
        Set<String> errors = new HashSet<>();
        Map<Role, Collection<Variable>> roleVarMap = headAtom.getRoleVarMap().asMap();
        Set<Role> duplicateRoles = roleVarMap.entrySet().stream().filter(entry -> entry.getValue().size() > 1).map(Map.Entry::getKey).collect(Collectors.toSet());

        // check that each pair of variables from the duplicate roles has a !=
        // otherwise there is a risk of having duplicate edges

        Set<Conjunction<Statement>> bodyPatterns = rule.when().getDisjunctiveNormalForm().getPatterns();

        ReasonerQueryImpl bodyQuery = ctx.queryFactory().create(Iterables.getOnlyElement(bodyPatterns));
        List<NeqIdPredicate> neqAssertions = bodyQuery.getAtoms(NeqIdPredicate.class).collect(Collectors.toList());

        for (Role duplicateRole : duplicateRoles) {
            Set<Variable> variables = new HashSet<>(roleVarMap.get(duplicateRole));
            Set<List<Variable>> variablePairsWithDuplicates = Sets.cartesianProduct(variables, variables);
            // we can remove all the duplicate pairs because (X,Y) and (Y,X) only need to be checked once
            // also remove pairs (X,X), these are over-generated
            Set<Set<Variable>> pairs = variablePairsWithDuplicates.stream().map(HashSet::new).filter(pair -> pair.size() > 1).collect(Collectors.toSet());
            for (Set<Variable> pair : pairs) {
                boolean requiredInequalityFound = neqAssertions.stream()
                        .anyMatch(neq -> neq.getVarNames().equals(pair));
                if (!requiredInequalityFound) {
                    errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_POSSIBLE_DUPLICATE_ROLE_PLAYER.getMessage(rule.label(), pair));
                }
            }
        }
        return errors;
    }
}
