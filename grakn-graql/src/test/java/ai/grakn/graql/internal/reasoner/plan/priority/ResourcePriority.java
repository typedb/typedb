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

package ai.grakn.graql.internal.reasoner.plan.priority;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.plan.SimplePlanner;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Class defining resolution weight for resource atoms.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class ResourcePriority extends BasePriority {

    ResourcePriority(Atom atom) {
        super(atom);
    }

    private boolean isSuperNode(){
        return atom().getParentQuery().tx().graql().match(atom().getCombinedPattern()).admin().stream()
                .skip(SimplePlanner.RESOURCE_SUPERNODE_SIZE)
                .findFirst().isPresent();
    }

    @Override
    public int computePriority(Set<Var> subbedVars){
        int priority = super.computePriority(subbedVars);
        Set<ai.grakn.graql.ValuePredicate> vps = atom().getPredicates(ValuePredicate.class).map(ValuePredicate::getPredicate).collect(Collectors.toSet());
        priority += SimplePlanner.IS_RESOURCE_ATOM;

        if (vps.isEmpty()) {
            if (subbedVars.contains(atom().getVarName()) || subbedVars.contains(atom().getPredicateVariable())
                    && !isSuperNode()) {
                priority += SimplePlanner.SPECIFIC_VALUE_PREDICATE;
            } else{
                priority += SimplePlanner.VARIABLE_VALUE_PREDICATE;
            }
        } else {
            int vpsPriority = 0;
            for (ai.grakn.graql.ValuePredicate vp : vps) {
                //vp with a value
                if (vp.isSpecific() && !isSuperNode()) {
                    vpsPriority += SimplePlanner.SPECIFIC_VALUE_PREDICATE;
                } //vp with a variable
                else if (vp.getInnerVar().isPresent()) {
                    VarPatternAdmin inner = vp.getInnerVar().orElse(null);
                    //variable mapped inside the query
                    if (subbedVars.contains(atom().getVarName())
                            || subbedVars.contains(inner.var())
                            && !isSuperNode()) {
                        vpsPriority += SimplePlanner.SPECIFIC_VALUE_PREDICATE;
                    } //variable equality
                    else if (vp.equalsValue().isPresent()){
                        vpsPriority += SimplePlanner.VARIABLE_VALUE_PREDICATE;
                    } //variable inequality
                    else {
                        vpsPriority += SimplePlanner.COMPARISON_VARIABLE_VALUE_PREDICATE;
                    }
                } else {
                    vpsPriority += SimplePlanner.NON_SPECIFIC_VALUE_PREDICATE;
                }
            }
            //normalise
            vpsPriority = vpsPriority/vps.size();
            priority += vpsPriority;
        }

        boolean reifiesRelation = atom().getImmediateNeighbours(Atom.class)
                .filter(Atom::isRelation)
                .anyMatch(at -> at.getVarName().equals(atom().getVarName()));

        priority += reifiesRelation ? SimplePlanner.RESOURCE_REIFYING_RELATION : 0;

        return priority;
    }

}
