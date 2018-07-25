/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.plan;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.OntologicalAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Resolution planner using {@link GreedyTraversalPlan} to establish optimal resolution order..
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class GraqlTraversalPlanner {

    /**
     *
     * Refined plan procedure:
     * - establish a list of starting atom candidates based on their substitutions
     * - create a plan using {@link GreedyTraversalPlan}
     * - if the graql plan picks an atom that is not a candidate
     *   - pick an optimal candidate
     *   - call the procedure on atoms with removed candidate
     * - otherwise return
     *
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using a refined {@link GraqlTraversal} procedure.
     */
    public static ImmutableList<Atom> plan(ReasonerQueryImpl query) {
        return ImmutableList.copyOf(refinePlan(query));
    }

    /**
     * optimise for:
     * - atoms with highest number of substitutions
     * @param candidates list of candidates
     * @return optimal candidate from the provided list according to the criteria above
     */
    @Nullable
    private static Atom optimalCandidate(List<Atom> candidates){
        return candidates.stream()
                .sorted(Comparator.comparing(at -> !at.isGround()))
                .sorted(Comparator.comparing(at -> -at.getPredicates().count()))
                .findFirst().orElse(null);
    }

    private static String PLACEHOLDER_ID = "placeholderId";

    private static List<Atom> refinePlan(ReasonerQueryImpl query){
        List<Atom> atoms = query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toList());
        Set<IdPredicate> subs = query.getAtoms(IdPredicate.class).collect(Collectors.toSet());

        ImmutableList<Atom> initialPlan = planFromTraversal(atoms, atomsToPattern(atoms, subs), query.tx());
        List<Atom> plan = refinePlan(query, initialPlan, subs);
        return plan;
    }

    /**
     * @param query top level query for which the plan is constructed
     * @param subs extra substitutions
     * @return an optimally ordered list of provided atoms
     */
    private static List<Atom> refinePlan(ReasonerQueryImpl query, List<Atom> initialPlan, Set<IdPredicate> subs){
        List<Atom> candidates = subs.isEmpty()?
                initialPlan :
                initialPlan.stream()
                        .filter(at -> at.getPredicates(IdPredicate.class).findFirst().isPresent())
                        .collect(Collectors.toList());

        Atom first = optimalCandidate(candidates);
        List<Atom> atomsToPlan = new ArrayList<>(initialPlan);
        if (first == null || first.equals(initialPlan.get(0))) {
            return initialPlan;
        } else {
            atomsToPlan.remove(first);

            Set<IdPredicate> extraSubs = first.getVarNames().stream()
                    .filter(v -> subs.stream().noneMatch(s -> s.getVarName().equals(v)))
                    .map(v -> IdPredicate.create(v, ConceptId.of(PLACEHOLDER_ID), query))
                    .collect(Collectors.toSet());

            Set<IdPredicate> totalSubs = Sets.union(subs, extraSubs);
            ImmutableList<Atom> partialPlan = planFromTraversal(atomsToPlan, atomsToPattern(atomsToPlan, totalSubs), query.tx());

            return Stream.concat(
                    Stream.of(first),
                    refinePlan(query, partialPlan, totalSubs).stream()
            ).collect(Collectors.toList());
        }
    }

    /**
     * @param atoms of interest
     * @param subs extra substitutions in the form of id predicates
     * @return conjunctive pattern composed of atoms + their constraints + subs
     */
    private static Conjunction<PatternAdmin> atomsToPattern(List<Atom> atoms, Set<IdPredicate> subs){
        return Patterns.conjunction(
                Stream.concat(
                        atoms.stream().flatMap(at -> Stream.concat(Stream.of(at), at.getNonSelectableConstraints())),
                        subs.stream()
                )
                        .map(Atomic::getCombinedPattern)
                        .flatMap(p -> p.admin().varPatterns().stream())
                        .collect(Collectors.toSet())
        );
    }

    /**
     *
     * @param atoms list of current atoms of interest
     * @param queryPattern corresponding pattern
     * @return an optimally ordered list of provided atoms
     */
    private static ImmutableList<Atom> planFromTraversal(List<Atom> atoms, PatternAdmin queryPattern, EmbeddedGraknTx<?> tx){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        atoms.stream()
                .filter(at -> !(at instanceof OntologicalAtom))
                .forEach(at -> at.getVarProperties().forEach(p -> propertyMap.put(p, at)));
        Set<VarProperty> properties = propertyMap.keySet();

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(queryPattern, tx);
        ImmutableList<Fragment> fragments = graqlTraversal.fragments().iterator().next();

        List<Atom> atomList = new ArrayList<>();
        atoms.stream().filter(at -> at instanceof OntologicalAtom).forEach(atomList::add);
        fragments.stream()
                .map(Fragment::varProperty)
                .filter(Objects::nonNull)
                .filter(properties::contains)
                .distinct()
                .flatMap(p -> propertyMap.get(p).stream())
                .distinct()
                .forEach(atomList::add);

        //add any unlinked items (disconnected and indexed for instance)
        propertyMap.values().stream()
                .filter(at -> !atomList.contains(at))
                .forEach(atomList::add);
        return ImmutableList.copyOf(atomList);
    }
}
