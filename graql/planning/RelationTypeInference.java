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

package grakn.core.graql.planning;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.graql.planning.gremlin.fragment.InIsaFragment;
import grakn.core.graql.planning.gremlin.fragment.InSubFragment;
import grakn.core.graql.planning.gremlin.fragment.LabelFragment;
import grakn.core.graql.planning.gremlin.fragment.OutRolePlayerFragment;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.IsaProperty;
import graql.lang.property.TypeProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static graql.lang.Graql.var;

public class RelationTypeInference {
    // infer type of relation type if we know the type of the role players
    // add label fragment and isa fragment if we can infer any
    public static Set<Fragment> inferRelationTypes(ConceptManager conceptManager, Set<Fragment> allFragments) {

        Set<Fragment> inferredFragments = new HashSet<>();

        Map<Variable, Type> labelVarTypeMap = getLabelVarTypeMap(conceptManager, allFragments);
        if (labelVarTypeMap.isEmpty()) return inferredFragments;

        Multimap<Variable, Type> instanceVarTypeMap = getInstanceVarTypeMap(allFragments, labelVarTypeMap);

        Multimap<Variable, Variable> relationRolePlayerMap = getRelationRolePlayerMap(allFragments, instanceVarTypeMap);
        if (relationRolePlayerMap.isEmpty()) return inferredFragments;

        // for each type, get all possible relation type it could be in
        Multimap<Type, RelationType> relationMap = HashMultimap.create();
        labelVarTypeMap.values().stream().distinct().forEach(
                type -> addAllPossibleRelations(relationMap, type));

        // inferred labels should be kept separately, even if they are already in allFragments set
        Map<Label, Statement> inferredLabels = new HashMap<>();
        relationRolePlayerMap.asMap().forEach((relationVar, rolePlayerVars) -> {

            Set<Type> possibleRelationTypes = rolePlayerVars.stream()
                    .filter(instanceVarTypeMap::containsKey)
                    .map(rolePlayer -> getAllPossibleRelationTypes(
                            instanceVarTypeMap.get(rolePlayer), relationMap))
                    .reduce(Sets::intersection).orElse(Collections.emptySet());

            //TODO: if possibleRelationTypes here is empty, the query will not match any data
            if (possibleRelationTypes.size() == 1) {

                Type relationType = possibleRelationTypes.iterator().next();
                Label label = relationType.label();

                // add label fragment if this label has not been inferred
                if (!inferredLabels.containsKey(label)) {
                    Statement labelVar = var();
                    inferredLabels.put(label, labelVar);
                    Fragment labelFragment = Fragments.label(new TypeProperty(label.getValue()), labelVar.var(), ImmutableSet.of(label));
                    inferredFragments.add(labelFragment);
                }

                // finally, add inferred isa fragments
                Statement labelVar = inferredLabels.get(label);
                IsaProperty isaProperty = new IsaProperty(labelVar);
                EquivalentFragmentSet isaEquivalentFragmentSet = EquivalentFragmentSets.isa(isaProperty,
                        relationVar, labelVar.var(), relationType.isImplicit());
                inferredFragments.addAll(isaEquivalentFragmentSet.fragments());
            }
        });

        return inferredFragments;
    }

    // find all vars with direct or indirect out isa edges
    private static Multimap<Variable, Type> getInstanceVarTypeMap(
            Set<Fragment> allFragments, Map<Variable, Type> labelVarTypeMap) {
        Multimap<Variable, Type> instanceVarTypeMap = HashMultimap.create();
        int oldSize;
        do {
            oldSize = instanceVarTypeMap.size();
            allFragments.stream()
                    .filter(fragment -> labelVarTypeMap.containsKey(fragment.start())) // restrict to types
                    .filter(fragment -> fragment instanceof InIsaFragment || fragment instanceof InSubFragment) //
                    .forEach(fragment -> instanceVarTypeMap.put(fragment.end(), labelVarTypeMap.get(fragment.start())));
        } while (oldSize != instanceVarTypeMap.size());
        return instanceVarTypeMap;
    }

    // find all vars representing types
    private static Map<Variable, Type> getLabelVarTypeMap(ConceptManager conceptManager, Set<Fragment> allFragments) {
        Map<Variable, Type> labelVarTypeMap = new HashMap<>();
        allFragments.stream()
                .filter(LabelFragment.class::isInstance)
                .forEach(fragment -> {
                    // TODO: labels() should return ONE label instead of a set
                    SchemaConcept schemaConcept = conceptManager.getSchemaConcept(
                            Iterators.getOnlyElement(((LabelFragment) fragment).labels().iterator()));
                    if (schemaConcept != null && !schemaConcept.isRole() && !schemaConcept.isRule()) {
                        labelVarTypeMap.put(fragment.start(), schemaConcept.asType());
                    }
                });
        return labelVarTypeMap;
    }

    private static Multimap<Variable, Variable> getRelationRolePlayerMap(
            Set<Fragment> allFragments, Multimap<Variable, Type> instanceVarTypeMap) {
        // get all relation vars and its role player vars
        Multimap<Variable, Variable> relationRolePlayerMap = HashMultimap.create();
        allFragments.stream().filter(OutRolePlayerFragment.class::isInstance)
                .forEach(fragment -> relationRolePlayerMap.put(fragment.start(), fragment.end()));

        Multimap<Variable, Variable> inferrableRelationsRolePlayerMap = HashMultimap.create();

        allFragments.stream()
                .filter(OutRolePlayerFragment.class::isInstance)
                .filter(fragment -> ! instanceVarTypeMap.containsKey(fragment.start())) // filter out known rel types
                .forEach(fragment -> {
                    Variable relation = fragment.start();

                    // the relation should have at least 2 known role players so we can infer something useful
                    int numRolePlayersHaveType = 0;
                    for (Variable rolePlayer : relationRolePlayerMap.get(relation)) {
                        if (instanceVarTypeMap.containsKey(rolePlayer)) {
                            numRolePlayersHaveType++;
                        }
                    }

                    if (numRolePlayersHaveType >= 2) {
                        inferrableRelationsRolePlayerMap.put(relation, fragment.end());
                    }
                });

        return inferrableRelationsRolePlayerMap;
    }

    private static void addAllPossibleRelations(Multimap<Type, RelationType> relationMap, Type metaType) {
        metaType.subs().forEach(type -> type.playing().flatMap(Role::relations)
                .forEach(relationType -> relationMap.put(type, relationType)));
    }

    private static Set<Type> getAllPossibleRelationTypes(
            Collection<Type> instanceVarTypes, Multimap<Type, RelationType> relationMap) {

        return instanceVarTypes.stream()
                .map(rolePlayerType -> (Set<Type>) new HashSet<Type>(relationMap.get(rolePlayerType)))
                .reduce(Sets::intersection).orElse(Collections.emptySet());
    }
}
