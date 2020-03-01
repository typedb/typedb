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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.AtomicBase;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.ReasonerException;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class defining the resolution plan for a given ReasonerQueryImpl at an atom level.
 * The plan is constructed  using the GraqlTraversal with the aid of GraqlTraversalPlanner.
 */
public final class ResolutionPlan {

    private final ImmutableList<Atom> plan;
    private final ReasonerQueryImpl query;
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionPlan.class);

    public ResolutionPlan(ReasonerQueryImpl q, TraversalPlanFactory traversalPlanFactory){
        this.query = q;
        this.plan = GraqlTraversalPlanner.plan(query, traversalPlanFactory);
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


    private void validatePlan() {
        if (!isComplete()){
            throw ReasonerException.incompleteResolutionPlan(query);
        }

        Iterator<Atom> iterator = plan.iterator();
        Set<Variable> vars = new HashSet<>(iterator.next().getVarNames());
        while (iterator.hasNext()) {
            Atom next = iterator.next();
            Set<Variable> varNames = next.getVarNames();
            boolean planDisconnected = Sets.intersection(varNames, vars).isEmpty();
            if (planDisconnected) {
                LOG.debug("Disconnected resolution plan produced:\n{}", this);
                break;
            }
            vars.addAll(varNames);
        }
    }
}

