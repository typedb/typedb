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

package grakn.core.graql.reasoner.plan;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.OntologicalAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Resolution planner using TraversalPlanner to establish optimal resolution order..
 */
public class GraqlTraversalPlanner {

    /**
     *
     * Refined plan procedure:
     * - establish a list of starting atom candidates based on their substitutions
     * - create a plan using TraversalPlanner
     * - if the graql plan picks an atom that is not a candidate
     *   - pick an optimal candidate
     *   - call the procedure on atoms with removed candidate
     * - otherwise return
     *
     * @param query for which the plan should be constructed
     * @return list of atoms in order they should be resolved using a refined GraqlTraversal procedure.
     */
    public static ImmutableList<Atom> plan(ReasonerQuery query, TraversalPlanFactory traversalPlanFactory) {
        return ImmutableList.copyOf(refinedPlan(query, traversalPlanFactory));
    }

    private static long atomPredicates(Atom at, Set<IdPredicate> subs){
        return Stream.concat(
                subs.stream().filter(sub -> !Sets.intersection(sub.getVarNames(), at.getVarNames()).isEmpty()),
                Stream.concat(
                        at.getPredicates(ValuePredicate.class),
                        at.getInnerPredicates(ValuePredicate.class)
                )
                        .filter(vp -> vp.getPredicate().isValueEquality())
        )
                .count();
    }

    private static List<Atom> optimiseCandidates(List<Atom> candidates, Set<IdPredicate> subs, Set<Variable> vars){
        return candidates.stream()
                .filter(at -> vars.isEmpty() || !Sets.intersection(at.getVarNames(), vars).isEmpty())
                .sorted(Comparator.comparing(at -> !at.isGround()))
                .sorted(Comparator.comparing(at -> -atomPredicates(at, subs)))
                .collect(Collectors.toList());
    }

    private static List<Atom> getCandidates(List<Atom> atoms, Set<IdPredicate> subs, Set<Variable> vars){
        List<Atom> preCandidates = optimiseCandidates(atoms, subs, vars);
        long MAX_CANDIDATES = preCandidates.size();
        if (MAX_CANDIDATES > 1) {
            boolean maxExists = atomPredicates(preCandidates.get(0), subs) > atomPredicates(preCandidates.get(1), subs);
            if (maxExists) MAX_CANDIDATES = 1;
        }
        return preCandidates.stream().limit(MAX_CANDIDATES).collect(Collectors.toList());
    }

    final private static String PLACEHOLDER_ID = "placeholderId";

    /**
     * @param query top level query for which the plan is constructed
     * @return an optimally ordered list of provided atoms
     */
    private static List<Atom> refinedPlan(ReasonerQuery query, TraversalPlanFactory traversalPlanFactory){
        List<Atom> atomsToProcess = query.getAtoms(Atom.class).filter(Atomic::isSelectable).collect(Collectors.toList());
        Set<IdPredicate> subs = query.getAtoms(IdPredicate.class).collect(Collectors.toSet());
        Set<Variable> vars = atomsToProcess.stream().anyMatch(Atom::isDisconnected)? query.getVarNames() : new HashSet<>();
        List<Atom> orderedAtoms = new ArrayList<>();

        while(!atomsToProcess.isEmpty()){
            List<Atom> candidates = getCandidates(atomsToProcess, subs, vars);
            ImmutableList<Atom> initialPlan = planFromTraversal(atomsToProcess, atomsToPattern(atomsToProcess, subs), traversalPlanFactory);

            if (candidates.contains(initialPlan.get(0)) || candidates.isEmpty()) {
                orderedAtoms.addAll(initialPlan);
                break;
            } else {
                Atom first = candidates.stream().findFirst().orElse(null);
                orderedAtoms.add(first);
                atomsToProcess.remove(first);

                Set<Variable> subVariables = subs.stream().flatMap(sub -> sub.getVarNames().stream()).collect(Collectors.toSet());
                first.getVarNames().stream()
                        .peek(vars::add)
                        .filter(v -> !subVariables.contains(v))
                        .map(v -> IdPredicate.create(v, ConceptId.of(PLACEHOLDER_ID), query))
                        .forEach(subs::add);
            }
        }
        return orderedAtoms;
    }

    /**
     * @param atoms of interest
     * @param subs extra substitutions in the form of id predicates
     * @return conjunctive pattern composed of atoms + their constraints + subs
     */
    public static Conjunction<Pattern> atomsToPattern(List<Atom> atoms, Set<IdPredicate> subs){
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
     * @param queryPattern corresponding (possible augmented with subs) pattern
     * @return an optimally ordered list of provided atoms
     */
    private static ImmutableList<Atom> planFromTraversal(List<Atom> atoms, Conjunction<?> queryPattern, TraversalPlanFactory planFactory){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        atoms.stream()
                .filter(atom -> !(atom instanceof OntologicalAtom))
                .forEach(atom -> atom.getVarProperties().forEach(property -> propertyMap.put(property, atom)));
        Set<VarProperty> properties = propertyMap.keySet();

        GraqlTraversal graqlTraversal = planFactory.createTraversal(queryPattern);
        ImmutableList<? extends Fragment> fragments = Iterables.getOnlyElement(graqlTraversal.fragments());

        List<Atom> atomList = new ArrayList<>();

        atoms.stream()
                .filter(atom -> atom instanceof OntologicalAtom)
                .forEach(atomList::add);

        fragments.stream()
                .map(Fragment::varProperty)
                .filter(Objects::nonNull)
                .filter(properties::contains)
                .distinct()
                .flatMap(property -> propertyMap.get(property).stream())
                .distinct()
                .forEach(atomList::add);

        //add any unlinked items (disconnected and indexed for instance)
        propertyMap.values().stream()
                .filter(at -> !atomList.contains(at))
                .forEach(atomList::add);
        return ImmutableList.copyOf(atomList);
    }
}
