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

package grakn.core.graql.reasoner.atom.task.infer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import grakn.common.util.Pair;
import grakn.core.common.util.Streams;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.util.ConceptUtils;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.graql.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.reasoner.utils.conversion.TypeConverter;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.property.RelationProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.concept.util.ConceptUtils.top;
import static graql.lang.Graql.var;

public class RelationTypeReasoner implements TypeReasoner<RelationAtom> {

    @Override
    public RelationAtom inferTypes(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        RelationAtom typedRelation = inferType(atom, sub, ctx);
        return inferRoles(typedRelation, sub, ctx);
    }

    /**
     * infer RelationTypes that this RelationAtom can potentially have
     * NB: EntityTypes and link Roles are treated separately as they behave differently:
     * NB: Not using Memoized as memoized methods can't have parameters
     * EntityTypes only play the explicitly defined Roles (not the relevant part of the hierarchy of the specified Role) and the Role inherited from parent
     *
     * @return list of RelationTypes this atom can have ordered by the number of compatible Roles
     */
    @Override
    public ImmutableList<Type> inferPossibleTypes(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        SchemaConcept type = atom.getSchemaConcept();
        if (type != null) return ImmutableList.of(type.asType());

        KeyspaceStatistics keyspaceStatistics = ctx.keyspaceStatistics();
        ConceptManager conceptManager = ctx.conceptManager();
        Multimap<RelationType, Role> compatibleConfigurations = inferPossibleRelationConfigurations(atom, sub, ctx);
        Set<Variable> untypedRoleplayers = Sets.difference(atom.getRolePlayers(), atom.getParentQuery().getVarTypeMap().keySet());
        Set<RelationAtom> untypedNeighbours = atom.getNeighbours(RelationAtom.class)
                .filter(at -> !Sets.intersection(at.getVarNames(), untypedRoleplayers).isEmpty())
                .collect(Collectors.toSet());

        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        //prioritise relations with higher chance of yielding answers
        compatibleConfigurations.asMap().entrySet().stream()
                //prioritise relations with more allowed roles
                .sorted(Comparator.comparing(e -> -e.getValue().size()))
                //prioritise relations with number of roles equal to arity
                .sorted(Comparator.comparing(e -> e.getKey().roles().count() != atom.getRelationPlayers().size()))
                //prioritise relations having more instances
                .sorted(Comparator.comparing(e -> -keyspaceStatistics.count(conceptManager, e.getKey().label())))
                //prioritise relations with highest number of possible types played by untyped role players
                .map(e -> {
                    if (untypedNeighbours.isEmpty()) return new Pair<>(e.getKey(), 0L);

                    Iterator<RelationAtom> neighbourIterator = untypedNeighbours.iterator();
                    Set<Type> typesFromNeighbour = inferPossibleEntityTypePlayers(neighbourIterator.next(), sub, ctx);
                    while (neighbourIterator.hasNext()) {
                        typesFromNeighbour = Sets.intersection(typesFromNeighbour, inferPossibleEntityTypePlayers(neighbourIterator.next(), sub, ctx));
                    }

                    Set<Role> rs = e.getKey().roles().collect(Collectors.toSet());
                    rs.removeAll(e.getValue());
                    return new Pair<>(
                            e.getKey(),
                            rs.stream().flatMap(Role::players).filter(typesFromNeighbour::contains).count()
                    );
                })
                .sorted(Comparator.comparing(p -> -p.second()))
                //prioritise non-implicit relations
                .sorted(Comparator.comparing(e -> e.first().isImplicit()))
                .map(Pair::first)
                //retain super types only
                .filter(t -> Sets.intersection(ConceptUtils.nonMetaSups(t), compatibleConfigurations.keySet()).isEmpty())
                .forEach(builder::add);
        //TODO need to add THING and meta relation type as well to make it complete
        return builder.build();
    }

    @Override
    public List<Atom> atomOptions(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        return inferPossibleTypes(atom, sub, ctx).stream()
                .map(atom::addType)
                .map(at -> inferTypes(at, sub, ctx))
                //order by number of distinct roles
                .sorted(Comparator.comparing(at -> -at.getRoleLabels().size()))
                .sorted(Comparator.comparing(at -> atom.isRuleResolvable()))
                .collect(Collectors.toList());
    }

    /**
     * infer RelationTypes that this RelationAtom can potentially have
     * NB: EntityTypes and link Roles are treated separately as they behave differently:
     * EntityTypes only play the explicitly defined Roles (not the relevant part of the hierarchy of the specified Role) and the Role inherited from parent
     *
     * @return list of RelationTypes this atom can have ordered by the number of compatible Roles
     */
    private Set<Type> inferPossibleEntityTypePlayers(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        return inferPossibleRelationConfigurations(atom, sub, ctx).asMap().entrySet().stream()
                .flatMap(e -> {
                    Set<Role> rs = e.getKey().roles().collect(Collectors.toSet());
                    rs.removeAll(e.getValue());
                    return rs.stream().flatMap(Role::players);
                }).collect(Collectors.toSet());
    }

    /**
     * @return a map of relations and corresponding roles that could be played by this atom
     */
    private Multimap<RelationType, Role> inferPossibleRelationConfigurations(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        Set<Role> roles = getExplicitRoles(atom, ctx).filter(r -> !Schema.MetaSchema.isMetaLabel(r.label())).collect(Collectors.toSet());
        SetMultimap<Variable, Type> varTypeMap = atom.getParentQuery().getVarTypeMap(sub);
        Set<Type> types = atom.getRolePlayers().stream().filter(varTypeMap::containsKey).flatMap(v -> varTypeMap.get(v).stream()).collect(Collectors.toSet());

        ConceptManager conceptManager = ctx.conceptManager();
        if (roles.isEmpty() && types.isEmpty()) {
            RelationType metaRelationType = conceptManager.getMetaRelationType();
            Multimap<RelationType, Role> compatibleTypes = HashMultimap.create();
            metaRelationType.subs()
                    .filter(rt -> !rt.equals(metaRelationType))
                    .forEach(rt -> compatibleTypes.putAll(rt, rt.roles().collect(Collectors.toSet())));
            return compatibleTypes;
        }

        //intersect relation types from roles and types
        Multimap<RelationType, Role> compatibleTypes;
        Multimap<RelationType, Role> compatibleTypesFromRoles = ReasonerUtils.compatibleRelationTypesWithRoles(roles, new RoleConverter());
        Multimap<RelationType, Role> compatibleTypesFromTypes = ReasonerUtils.compatibleRelationTypesWithRoles(types, new TypeConverter());

        if (roles.isEmpty()) {
            compatibleTypes = compatibleTypesFromTypes;
        }
        //no types from roles -> roles correspond to mutually exclusive relations
        else if (compatibleTypesFromRoles.isEmpty() || types.isEmpty()) {
            compatibleTypes = compatibleTypesFromRoles;
        } else {
            compatibleTypes = ReasonerUtils.multimapIntersection(compatibleTypesFromTypes, compatibleTypesFromRoles);
        }
        return compatibleTypes;
    }

    private Stream<Role> getExplicitRoles(RelationAtom atom, ReasoningContext ctx) {
        ConceptManager conceptManager = ctx.conceptManager();
        return atom.getRelationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole)
                .flatMap(Streams::optionalToStream)
                .map(Statement::getType)
                .flatMap(Streams::optionalToStream)
                .map(conceptManager::getRole)
                .filter(Objects::nonNull);
    }

    /**
     * attempt to infer the relation type of this relation
     *
     * @param sub extra instance information to aid entity type inference
     * @return either this if relation type can't be inferred or a fresh relation with inferred relation type
     */
    private RelationAtom inferType(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        if (atom.getTypeLabel() != null) return atom;
        if (sub.containsVar(atom.getPredicateVariable())) return atom.addType(sub.get(atom.getPredicateVariable()).asType());
        List<Type> relationTypes = inferPossibleTypes(atom, sub, ctx);
        if (relationTypes.size() == 1) return atom.addType(Iterables.getOnlyElement(relationTypes));
        return atom;
    }

    /**
     * attempt to infer role types of this relation and return a fresh relation with inferred role types
     *
     * @return either this if nothing/no roles can be inferred or fresh relation with inferred role types
     */
    private RelationAtom inferRoles(RelationAtom atom, ConceptMap sub, ReasoningContext ctx) {
        //return if all roles known and non-meta
        List<Role> explicitRoles = getExplicitRoles(atom, ctx).collect(Collectors.toList());
        SetMultimap<Variable, Type> varTypeMap = atom.getParentQuery().getVarTypeMap(sub);
        boolean allRolesMeta = explicitRoles.stream().allMatch(role -> Schema.MetaSchema.isMetaLabel(role.label()));
        boolean roleRecomputationViable = allRolesMeta && (!sub.isEmpty() || !Sets.intersection(varTypeMap.keySet(), atom.getRolePlayers()).isEmpty());
        if (explicitRoles.size() == atom.getRelationPlayers().size() && !roleRecomputationViable) return atom;

        ConceptManager conceptManager = ctx.conceptManager();
        Role metaRole = conceptManager.getMetaRole();
        List<RelationProperty.RolePlayer> allocatedRelationPlayers = new ArrayList<>();
        SchemaConcept schemaConcept = atom.getSchemaConcept();
        RelationType relType = null;
        if (schemaConcept != null && schemaConcept.isRelationType()) relType = schemaConcept.asRelationType();

        //explicit role types from castings
        List<RelationProperty.RolePlayer> inferredRelationPlayers = new ArrayList<>();
        atom.getRelationPlayers().forEach(rp -> {
            Variable varName = rp.getPlayer().var();
            Statement rolePattern = rp.getRole().orElse(null);
            if (rolePattern != null) {
                String roleLabel = rolePattern.getType().orElse(null);
                //allocate if variable role or if label non meta
                if (roleLabel == null || !Schema.MetaSchema.isMetaLabel(Label.of(roleLabel))) {
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
                    allocatedRelationPlayers.add(rp);
                }
            }
        });

        //remaining roles
        //role types can repeat so no matter what has been allocated still the full spectrum of possibilities is present
        //TODO make restrictions based on cardinality constraints
        Set<Role> possibleRoles = relType != null ?
                relType.roles().collect(Collectors.toSet()) :
                inferPossibleTypes(atom, sub, ctx).stream()
                        .filter(Concept::isRelationType)
                        .map(Concept::asRelationType)
                        .flatMap(RelationType::roles).collect(Collectors.toSet());

        //possible role types for each casting based on its type
        Map<RelationProperty.RolePlayer, Set<Role>> mappings = new HashMap<>();
        atom.getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Variable varName = rp.getPlayer().var();
                    Set<Type> types = varTypeMap.get(varName);
                    mappings.put(rp, top(ReasonerUtils.compatibleRoles(types, possibleRoles)));
                });


        //allocate all unambiguous mappings
        mappings.entrySet().stream()
                .filter(entry -> entry.getValue().size() == 1)
                .forEach(entry -> {
                    RelationProperty.RolePlayer rp = entry.getKey();
                    Variable varName = rp.getPlayer().var();
                    Role role = Iterables.getOnlyElement(entry.getValue());
                    Statement rolePattern = var().type(role.label().getValue());
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
                    allocatedRelationPlayers.add(rp);
                });

        //fill in unallocated roles with metarole
        atom.getRelationPlayers().stream()
                .filter(rp -> !allocatedRelationPlayers.contains(rp))
                .forEach(rp -> {
                    Variable varName = rp.getPlayer().var();
                    Statement rolePattern = rp.getRole().orElse(null);

                    rolePattern = rolePattern != null ?
                            rolePattern.type(metaRole.label().getValue()) :
                            var().type(metaRole.label().getValue());
                    inferredRelationPlayers.add(new RelationProperty.RolePlayer(rolePattern, new Statement(varName)));
                });

        Statement relationPattern = RelationAtom.relationPattern(atom.getVarName(), inferredRelationPlayers);
        Statement newPattern =
                (atom.isDirect() ?
                        relationPattern.isaX(new Statement(atom.getPredicateVariable())) :
                        relationPattern.isa(new Statement(atom.getPredicateVariable()))
                );

        return RelationAtom.create(newPattern, atom.getPredicateVariable(), atom.getTypeLabel(), atom.getPossibleTypes(), atom.getParentQuery(), atom.context());
    }
}
