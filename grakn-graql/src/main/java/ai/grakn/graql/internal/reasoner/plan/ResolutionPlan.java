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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl} at an atom level.
 * The plan is constructed using the {@link GraqlTraversal} with the aid of {@link GraqlTraversalPlanner}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public final class ResolutionPlan {

    final private ImmutableList<Atom> plan;
    final private ReasonerQueryImpl query;

    public ResolutionPlan(ReasonerQueryImpl q){
        this.query = q;
        this.plan = plan(query);
        validatePlan();
    }

    @Override
    public String toString(){
        return plan.stream().map(AtomicBase::toString).collect(Collectors.joining("\n"));
    }

    /**
     * @return corresponding atom plan
     */
    public ImmutableList<Atom> plan(){ return plan;}

    /**
     * @return true if the plan is complete with respect to provided query - contains all selectable atoms
     */
    private boolean isComplete(){
        return query.selectAtoms().allMatch(plan::contains);
    }

    /**
     * @return true if the plan is valid with respect to provided query - its resolution doesn't lead to any non-ground neq predicates
     */
    private boolean isNeqGround(){
        Set<NeqPredicate> nonGroundPredicates = new HashSet<>();
        Set<Var> mappedVars = this.query.getAtoms(IdPredicate.class).map(Atomic::getVarName).collect(Collectors.toSet());
        for(Atom atom : this.plan){
            mappedVars.addAll(atom.getVarNames());
            atom.getPredicates(NeqPredicate.class)
                    .forEach(neq -> {
                        //look for non-local non-ground predicates
                        if (!mappedVars.containsAll(neq.getVarNames())
                                && !atom.getVarNames().containsAll(neq.getVarNames())){
                            nonGroundPredicates.add(neq);
                        } else{
                            //if this is ground for this atom but non-ground for another it is ground
                            if (nonGroundPredicates.contains(neq)) nonGroundPredicates.remove(neq);
                        }
                    });
        }
        return nonGroundPredicates.isEmpty();
    }

    private void validatePlan() {
        if (!isNeqGround()) {
            throw GraqlQueryException.nonGroundNeqPredicate(query);
        }
        if (!isComplete()){
            throw GraqlQueryException.incompleteResolutionPlan(query);
        }
    }

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
    private static ImmutableList<Atom> plan(ReasonerQueryImpl query) {
        return ImmutableList.copyOf(refinePlan(query));
    }


    private static Stream<Atom> optimiseAtoms(Stream<Atom> atoms){
        return atoms
                .sorted(Comparator.comparing(at -> !at.isGround()))
                .sorted(Comparator.comparing(at -> -at.getPredicates().count()));
    }

    private static Stream<Atom> optimiseRelations(Stream<Atom> atoms){
        return optimiseAtoms(
                atoms
                        .sorted(Comparator.comparing(at -> at.getImmediateNeighbours(Atom.class).filter(Atom::isSelectable).count()))
                        .sorted(Comparator.comparing(Atom::isRuleResolvable))
        );
    }

    /**
     * optimise for:
     * - atoms with highest number of substitutions
     * - if an optimal atom is a relation, optimise for relations:
     *      - prioritising with least number of selectable neighbours
     *      - prioritising non-resolvable relations
     * @param candidates list of candidates
     * @return optimal candidate from the provided list according to the criteria above
     */
    @Nullable
    private static Atom optimalCandidate(List<Atom> candidates){
        Atom atom = optimiseAtoms(candidates.stream()).findFirst().orElse(null);
        if (atom == null || !atom.isRelation()) return atom;
        return optimiseRelations(candidates.stream().filter(Atom::isRelation)).findFirst().orElse(null);
    }

    private static String PLACEHOLDER_ID = "placeholderId";

    private static List<Atom> refinePlan(ReasonerQueryImpl query){
        List<Atom> atoms = query.getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toList());
        Set<IdPredicate> subs = query.getAtoms(IdPredicate.class).collect(Collectors.toSet());

        ImmutableList<Atom> initialPlan = GraqlTraversalPlanner.planFromTraversal(atoms, subs, query.tx());
        return refinePlan(query, initialPlan, subs);
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
            ImmutableList<Atom> partialPlan = GraqlTraversalPlanner.planFromTraversal(atomsToPlan, totalSubs, query.tx());

            return Stream.concat(
                    Stream.of(first),
                    refinePlan(query, partialPlan, totalSubs).stream()
            ).collect(Collectors.toList());
        }
    }
}

