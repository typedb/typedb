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

package grakn.core.graql.reasoner.atom.task.relate;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import grakn.common.util.Pair;
import grakn.core.common.util.ListsUtil;
import grakn.core.common.util.Streams;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.cache.VariableDefinition;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.property.RelationProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RelationSemanticProcessor implements SemanticProcessor<RelationAtom> {

    private final TypeAtomSemanticProcessor binarySemanticProcessor = new TypeAtomSemanticProcessor();

    @Override
    public Unifier getUnifier(RelationAtom childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        return getMultiUnifier(childAtom, parentAtom, unifierType, ctx).getUnifier();
    }

    @Override
    public MultiUnifier getMultiUnifier(RelationAtom childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        Unifier baseUnifier = binarySemanticProcessor.getUnifier(childAtom.toIsaAtom(), parentAtom, unifierType, ctx);
        if (baseUnifier == null) {
            return MultiUnifierImpl.nonExistent();
        }

        Set<Unifier> unifiers = new HashSet<>();
        if (parentAtom.isRelationAtom()) {
            RelationAtom parent = (RelationAtom) parentAtom;
            Set<List<Pair<RelationProperty.RolePlayer, RelationProperty.RolePlayer>>> rpMappings = getRelationPlayerMappings(childAtom, parent, unifierType, ctx);
            boolean containsRoleVariables = parent.getRelationPlayers().stream()
                    .map(RelationProperty.RolePlayer::getRole)
                    .flatMap(Streams::optionalToStream)
                    .anyMatch(rp -> rp.var().isReturned());

            //NB: if two atoms are equal and their rp mappings are complete we return the identity unifier
            //this is important for cases like unifying ($r1: $x, $r2: $y) with itself
            //this is only for cached queries to ensure they do not produce spurious answers
            if (containsRoleVariables
                    && unifierType != UnifierType.RULE
                    //for subsumptive unifiers we need a meaningful (with actual variables) inverse
                    && unifierType != UnifierType.SUBSUMPTIVE
                    && !rpMappings.isEmpty()
                    && rpMappings.stream().allMatch(mapping -> mapping.size() == childAtom.getRelationPlayers().size())) {
                boolean queriesEqual = ReasonerQueryEquivalence.Equality.equivalent(childAtom.getParentQuery(), parent.getParentQuery());
                if (queriesEqual) return MultiUnifierImpl.trivial();
            }

            rpMappings
                    .forEach(mappingList -> {
                        Multimap<Variable, Variable> varMappings = HashMultimap.create();
                        mappingList.forEach(rpm -> {
                            //add role player mapping
                            varMappings.put(rpm.first().getPlayer().var(), rpm.second().getPlayer().var());

                            //add role var mapping if needed
                            Statement childRolePattern = rpm.first().getRole().orElse(null);
                            Statement parentRolePattern = rpm.second().getRole().orElse(null);
                            if (parentRolePattern != null && childRolePattern != null && containsRoleVariables) {
                                varMappings.put(childRolePattern.var(), parentRolePattern.var());
                            }

                        });
                        unifiers.add(baseUnifier.merge(new UnifierImpl(varMappings)));
                    });
        } else {
            unifiers.add(baseUnifier);
        }

        if (!unifierType.allowsNonInjectiveMappings()
                && unifiers.stream().anyMatch(Unifier::isNonInjective)) {
            return MultiUnifierImpl.nonExistent();
        }
        return new MultiUnifierImpl(unifiers);
    }

    /**
     * @param parentAtom  reference atom defining the mapping
     * @param unifierType type of match to be performed
     * @return set of possible COMPLETE mappings between this (child) and parent relation players
     */
    private Set<List<Pair<RelationProperty.RolePlayer, RelationProperty.RolePlayer>>> getRelationPlayerMappings(RelationAtom childAtom, RelationAtom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        SetMultimap<Variable, Type> childVarTypeMap = childAtom.getParentQuery().getVarTypeMap(unifierType.inferTypes());
        SetMultimap<Variable, Type> parentVarTypeMap = parentAtom.getParentQuery().getVarTypeMap(unifierType.inferTypes());

        //TODO:: consider checking consistency wrt the schema (type compatibility, playability, etc)
        //TODO:: of the (atom+parent) conjunction similarly to what we do at commit-time validation

        //establish compatible castings for each parent casting
        List<Set<Pair<RelationProperty.RolePlayer, RelationProperty.RolePlayer>>> compatibleMappingsPerParentRP = new ArrayList<>();

        //child query is rule body + head here
        ReasonerQuery childQuery = childAtom.getParentQuery();
        ConceptManager conceptManager = ctx.conceptManager();
        parentAtom.getRelationPlayers()
                .forEach(prp -> {
                    Statement parentRolePattern = prp.getRole().orElse(null);
                    if (parentRolePattern == null) throw ReasonerException.rolePatternAbsent(parentAtom);

                    String parentRoleLabel = parentRolePattern.getType().isPresent() ? parentRolePattern.getType().get() : null;
                    Role parentRole = parentRoleLabel != null ? conceptManager.getRole(parentRoleLabel) : null;
                    Variable parentRolePlayer = prp.getPlayer().var();
                    Set<Type> parentTypes = parentVarTypeMap.get(parentRolePlayer);
                    Type parentTypeExact = parentAtom.getPredicates(parentRolePlayer, IdPredicate.class)
                            .filter(idPredicate ->  !idPredicate.isPlaceholder())
                            .findAny()
                            .map(idPredicate -> (Concept)ctx.conceptManager().getConcept(idPredicate.getPredicate()))
                            .filter(Concept::isThing)
                            .map(concept -> concept.asThing().type())
                            .orElse(null);

                    Set<RelationProperty.RolePlayer> compatibleRelationPlayers = new HashSet<>();
                    childAtom.getRelationPlayers().stream()
                            //check for role compatibility
                            .filter(crp -> {
                                Statement childRolePattern = crp.getRole().orElse(null);
                                if (childRolePattern == null) throw ReasonerException.rolePatternAbsent(childAtom);

                                String childRoleLabel = childRolePattern.getType().isPresent() ? childRolePattern.getType().get() : null;
                                Role childRole = childRoleLabel != null ? conceptManager.getRole(childRoleLabel) : null;

                                boolean varCompatibility = unifierType.equivalence() == null
                                        || parentRolePattern.var().isReturned() == childRolePattern.var().isReturned();

                                // if we have IDs for both variables' types for the role players
                                // we use that for checking role compatibility rather than the type labels
                                // in other words, we allow ID compatibility to take precedence over role type compatibility
                                // TODO, in the future, the behaviors should be explicit and aligned
                                boolean checkedByIdCompatibility = false;
                                if (crp.getRole().isPresent() && prp.getRole().isPresent()) {
                                    Variable childRPVar = crp.getRole().get().var();
                                    Variable parentRPVar = prp.getRole().get().var();
                                    checkedByIdCompatibility = parentAtom.getAllPredicates(parentRPVar, IdPredicate.class).findAny().isPresent() &&
                                            childAtom.getAllPredicates(childRPVar, IdPredicate.class).findAny().isPresent();
                                }
                                return checkedByIdCompatibility || (varCompatibility && unifierType.roleCompatibility(parentRole, childRole));
                            })
                            //check for inter-type compatibility
                            .filter(crp -> {
                                Variable childVar = crp.getPlayer().var();
                                Set<Type> childTypes = childVarTypeMap.get(childVar);

                                return unifierType.typeCompatibility(parentTypes, childTypes)
                                        && unifierType.typePlayabilityWithInsertSemantics(childAtom, childVar, parentTypes, parentTypeExact);
                            })
                            //rule body playability - match semantics
                            .filter(crp -> {
                                Variable childVar = crp.getPlayer().var();
                                return childQuery.getAtoms(RelationAtom.class)
                                        .filter(at -> !at.equals(childAtom))
                                        .allMatch(at -> unifierType.typePlayabilityWithMatchSemantics(childAtom, childVar, parentTypes, parentTypeExact));
                            })
                            //check for substitution compatibility
                            .filter(crp -> {
                                Set<Atomic> parentIds = parentAtom.getPredicates(prp.getPlayer().var(), IdPredicate.class).collect(Collectors.toSet());
                                Set<Atomic> childIds = childAtom.getPredicates(crp.getPlayer().var(), IdPredicate.class).collect(Collectors.toSet());

                                Set<Atomic> parentRoleIds = new HashSet<>();
                                if (prp.getRole().isPresent()) {
                                    parentAtom.getAllPredicates(prp.getRole().get().var(), IdPredicate.class).forEach(parentRoleIds::add);
                                }

                                Set<Atomic> childRoleIds = new HashSet<>();
                                if (crp.getRole().isPresent()) {
                                    childAtom.getAllPredicates(crp.getRole().get().var(), IdPredicate.class).forEach(childRoleIds::add);
                                }

                                return unifierType.idCompatibility(parentIds, childIds) && unifierType.idCompatibility(parentRoleIds, childRoleIds);
                            })
                            //check for value predicate compatibility
                            .filter(crp -> {
                                Set<Atomic> parentVP = parentAtom.getPredicates(prp.getPlayer().var(), ValuePredicate.class).collect(Collectors.toSet());
                                Set<Atomic> childVP = childAtom.getPredicates(crp.getPlayer().var(), ValuePredicate.class).collect(Collectors.toSet());
                                return unifierType.valueCompatibility(parentVP, childVP);
                            })
                            //TODO check substitution roleplayer connectedness
                            .forEach(compatibleRelationPlayers::add);

                    if (!compatibleRelationPlayers.isEmpty()) {
                        compatibleMappingsPerParentRP.add(
                                compatibleRelationPlayers.stream()
                                        .map(crp -> new Pair<>(crp, prp))
                                        .collect(Collectors.toSet())
                        );
                    }
                });

        return Sets.cartesianProduct(compatibleMappingsPerParentRP).stream()
                .filter(list -> !list.isEmpty())
                //check the same child rp is not mapped to multiple parent rps
                .filter(list -> {
                    List<RelationProperty.RolePlayer> listChildRps = list.stream().map(Pair::first).collect(Collectors.toList());
                    //NB: this preserves cardinality instead of removing all occurring instances which is what we want
                    return ListsUtil.listDifference(listChildRps, childAtom.getRelationPlayers()).isEmpty();
                })
                //check all parent rps mapped
                .filter(list -> {
                    List<RelationProperty.RolePlayer> listParentRps = list.stream().map(Pair::second).collect(Collectors.toList());
                    return listParentRps.containsAll(parentAtom.getRelationPlayers());
                })
                .collect(Collectors.toSet());
    }

    @Override
    public SemanticDifference computeSemanticDifference(RelationAtom parent, Atom child, Unifier unifier, ReasoningContext ctx) {
        SemanticDifference baseDiff = binarySemanticProcessor.computeSemanticDifference(parent.toIsaAtom(), child, unifier, ctx);

        if (!child.isRelationAtom()) return baseDiff;
        RelationAtom childAtom = (RelationAtom) child;
        Set<VariableDefinition> diff = new HashSet<>();

        ConceptManager conceptManager = ctx.conceptManager();
        Set<Variable> parentRoleVars = parent.getRoleExpansionVariables();
        ListMultimap<Variable, Role> childVarRoleMap = childAtom.getVarRoleMap();
        ListMultimap<Variable, Role> parentVarRoleMap = parent.getVarRoleMap();
        unifier.mappings().forEach(m -> {
            Variable childVar = m.getValue();
            Variable parentVar = m.getKey();
            Role requiredRole = null;
            if (parentRoleVars.contains(parentVar)) {
                Set<Label> roleLabels = childAtom.getRelationPlayers().stream()
                        .map(RelationProperty.RolePlayer::getRole)
                        .flatMap(Streams::optionalToStream)
                        .filter(roleStatement -> roleStatement.var().equals(childVar))
                        .map(Statement::getType)
                        .flatMap(Streams::optionalToStream)
                        .map(Label::of)
                        .collect(Collectors.toSet());
                if (!roleLabels.isEmpty()) {
                    requiredRole = conceptManager.getRole(Iterables.getOnlyElement(roleLabels).getValue());
                }
            }
            List<Role> childRoles = childVarRoleMap.get(childVar);
            List<Role> parentRoles = parentVarRoleMap.get(parentVar);
            // if the child and parent roles are exactly the same, the semantic difference is exactly 0
            if (ListsUtil.listDifference(childRoles, parentRoles).isEmpty()) {
                diff.add(new VariableDefinition(parentVar, null, requiredRole, new ArrayList<>(), new HashSet<>()));
            } else {
                // if the child and parent roles are not exactly the same, then we check all required role players
                // this is because we can't distinguish using a semantic difference, the following:
                // parent: 1 role player, child: 2 repeated role players
                // vs
                // parent: 2 role player, child: 3 repeated role players
                // both would end up with a single required role. Applying this semantic difference could be satisfied
                // by either case. We therefore explicitly require N role players to be repeatedly present
                // this is less efficient, but also less common
                List<Role> filteredChildRoles = childRoles.stream()
                    .filter(playedRole -> !Schema.MetaSchema.isMetaLabel(playedRole.label()))
                    .collect(Collectors.toList());
                diff.add(new VariableDefinition(parentVar, null, requiredRole, filteredChildRoles, new HashSet<>()));
            }
        });
        return baseDiff.merge(new SemanticDifference(diff));
    }
}
