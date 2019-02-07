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

import com.google.common.collect.ImmutableList;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicBase;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.query.pattern.statement.Variable;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class defining the resolution plan for a given {@link ReasonerQueryImpl} at an atom level.
 * The plan is constructed  using the {@link GraqlTraversal} with the aid of {@link GraqlTraversalPlanner}.
 */
public final class ResolutionPlan {

    final private ImmutableList<Atom> plan;
    final private ReasonerQueryImpl query;

    public ResolutionPlan(ReasonerQueryImpl q){
        this.query = q;
        this.plan = GraqlTraversalPlanner.plan(query);
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
        Set<Variable> mappedVars = this.query.getAtoms(IdPredicate.class).map(Atomic::getVarName).collect(Collectors.toSet());
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

}

