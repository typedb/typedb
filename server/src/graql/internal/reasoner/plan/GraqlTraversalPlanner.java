/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.reasoner.plan;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.atom.binary.OntologicalAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.property.VarProperty;
import grakn.core.server.session.TransactionOLTP;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Resolution planner using {@link GreedyTraversalPlan} to establish optimal resolution order..
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
        List<Atom> startCandidates = query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toList());
        Set<IdPredicate> subs = query.getAtoms(IdPredicate.class).collect(Collectors.toSet());
        return ImmutableList.copyOf(refinePlan(query, startCandidates, subs));
    }

    @Nullable
    private static Atom optimalCandidate(List<Atom> candidates){
        return candidates.stream()
                .sorted(Comparator.comparing(at -> !at.isGround()))
                .sorted(Comparator.comparing(at -> -at.getPredicates().count()))
                .findFirst().orElse(null);
    }

    private static String PLACEHOLDER_ID = "placeholderId";

    /**
     * @param query top level query for which the plan is constructed
     * @param atoms list of current atoms of interest
     * @param subs extra substitutions
     * @return an optimally ordered list of provided atoms
     */
    private static List<Atom> refinePlan(ReasonerQueryImpl query, List<Atom> atoms, Set<IdPredicate> subs){
        List<Atom> candidates = subs.isEmpty()?
                atoms :
                atoms.stream()
                        .filter(at -> at.getPredicates(IdPredicate.class).findFirst().isPresent())
                        .collect(Collectors.toList());

        ImmutableList<Atom> initialPlan = planFromTraversal(atoms, atomsToPattern(atoms, subs), query.tx());
        if (candidates.contains(initialPlan.get(0)) || candidates.isEmpty()) {
            return initialPlan;
        } else {
            Atom first = optimalCandidate(candidates);
            List<Atom> atomsToPlan = new ArrayList<>(atoms);

            if(first != null){
                atomsToPlan.remove(first);

                Set<IdPredicate> extraSubs = first.getVarNames().stream()
                        .filter(v -> subs.stream().noneMatch(s -> s.getVarName().equals(v)))
                        .map(v -> IdPredicate.create(v, ConceptId.of(PLACEHOLDER_ID), query))
                        .collect(Collectors.toSet());

                return Stream.concat(
                        Stream.of(first),
                        refinePlan(query, atomsToPlan, Sets.union(subs, extraSubs)).stream()
                ).collect(Collectors.toList());
            } else {
                return refinePlan(query, atomsToPlan, subs);
            }
        }
    }

    /**
     * @param atoms of interest
     * @param subs extra substitutions in the form of id predicates
     * @return conjunctive pattern composed of atoms + their constraints + subs
     */
    private static Conjunction<Pattern> atomsToPattern(List<Atom> atoms, Set<IdPredicate> subs){
        return Graql.and(
                Stream.concat(
                        atoms.stream().flatMap(at -> Stream.concat(Stream.of(at), at.getNonSelectableConstraints())),
                        subs.stream()
                )
                        .map(Atomic::getCombinedPattern)
                        .flatMap(p -> p.statements().stream())
                        .collect(Collectors.toSet())
        );
    }

    /**
     *
     * @param atoms list of current atoms of interest
     * @param queryPattern corresponding pattern
     * @return an optimally ordered list of provided atoms
     */
    private static ImmutableList<Atom> planFromTraversal(List<Atom> atoms, Conjunction<?> queryPattern, TransactionOLTP tx){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        atoms.stream()
                .filter(atom -> !(atom instanceof OntologicalAtom))
                .forEach(atom -> atom.getVarProperties().forEach(property -> propertyMap.put(property, atom)));
        Set<VarProperty> properties = propertyMap.keySet();

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(queryPattern, tx);
        ImmutableList<Fragment> fragments = Iterables.getOnlyElement(graqlTraversal.fragments());

        List<Atom> atomList = new ArrayList<>();

        atoms.stream()
                .filter(atom -> atom instanceof OntologicalAtom)
                .forEach(atom -> atomList.add(atom));

        fragments.stream()
                .map(fragment -> fragment.varProperty())
                .filter(property -> Objects.nonNull(property))
                .filter(property -> properties.contains(property))
                .distinct()
                .flatMap(property -> propertyMap.get(property).stream())
                .distinct()
                .forEach(atom -> atomList.add(atom));

        //add any unlinked items (disconnected and indexed for instance)
        propertyMap.values().stream()
                .filter(at -> !atomList.contains(at))
                .forEach(atomList::add);
        return ImmutableList.copyOf(atomList);
    }
}
