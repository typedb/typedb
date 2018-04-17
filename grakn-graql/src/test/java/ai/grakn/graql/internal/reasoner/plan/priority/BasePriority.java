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
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.plan.SimplePlanner;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Class defining base resolution weight.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class BasePriority{

    private final Atom atom;

    BasePriority(Atom atom){
        this.atom = atom;
    }

    protected Atom atom(){ return atom;}

    public int computePriority(){
        return computePriority(atom.getPartialSubstitutions().map(IdPredicate::getVarName).collect(Collectors.toSet()));
    }

    /**
     * compute resolution priority based on provided substitution variables
     * @param subbedVars variables having a substitution
     * @return resolution priority value
     */
    protected int computePriority(Set<Var> subbedVars){
        int priority = 0;
        Set<Var> varNames = atom.getVarNames();
        priority += Sets.intersection(varNames, subbedVars).size() * SimplePlanner.PARTIAL_SUBSTITUTION;
        priority += atom.isRuleResolvable()? SimplePlanner.RULE_RESOLVABLE_ATOM : 0;
        priority += atom.isRecursive()? SimplePlanner.RECURSIVE_ATOM : 0;

        priority += atom.getTypeConstraints().count() * SimplePlanner.GUARD;
        Set<Var> otherVars = atom.getParentQuery().getAtoms().stream()
                .filter(a -> a != this)
                .flatMap(at -> at.getVarNames().stream())
                .collect(Collectors.toSet());
        priority += Sets.intersection(varNames, otherVars).size() * SimplePlanner.BOUND_VARIABLE;

        //inequality predicates with unmapped variable
        priority += atom.getPredicates(NeqPredicate.class)
                .map(Predicate::getPredicate)
                .filter(v -> !subbedVars.contains(v)).count() * SimplePlanner.INEQUALITY_PREDICATE;
        return priority;
    }

}
