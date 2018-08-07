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

package ai.grakn.graql.internal.reasoner.plan;

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
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
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
class GraqlTraversalPlanner {

    /**
     * @param atoms list of current atoms of interest
     * @param subs present substitutions
     * @return an optimally ordered list of provided atoms
     */
    public static ImmutableList<Atom> planFromTraversal(List<Atom> atoms, Set<IdPredicate> subs, EmbeddedGraknTx<?> tx){
        Multimap<VarProperty, Atom> propertyMap = HashMultimap.create();
        atoms.stream()
                .filter(at -> !(at instanceof OntologicalAtom))
                .forEach(at -> at.getVarProperties().forEach(p -> propertyMap.put(p, at)));
        Set<VarProperty> properties = propertyMap.keySet();

        PatternAdmin queryPattern = atomsToPattern(atoms, subs);
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

}
