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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.NeqProperty;

import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;
import java.util.Set;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an inequality predicate. Corresponds to graql {@link NeqProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class NeqPredicate extends Predicate<Var> {

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();
    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract Var getPredicate();


    public static NeqPredicate create(VarPattern pattern, ReasonerQuery parent) {
        return new AutoValue_NeqPredicate(pattern.admin().var(), pattern, parent, extractPredicate(pattern));
    }
    public static NeqPredicate create(Var varName, NeqProperty prop, ReasonerQuery parent) {
        VarPatternAdmin pattern = varName.neq(prop.var().var()).admin();
        return create(pattern, parent);
    }
    public static NeqPredicate create(NeqPredicate a, ReasonerQuery parent) {
        return create(a.getPattern(), parent);
    }

    private static Var extractPredicate(VarPattern pattern) {
        return pattern.admin().getProperties(NeqProperty.class).iterator().next().var().var();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public String toString(){ return "[" + getVarName() + "!=" + getReferenceVarName() + "]";}

    @Override
    public String getPredicateValue() {
        return getPredicate().getValue();
    }

    @Override
    public Set<Var> getVarNames(){
        Set<Var> vars = super.getVarNames();
        vars.add(getReferenceVarName());
        return vars;
    }

    /**
     * @param sub substitution to be checked against the predicate
     * @return true if provided subsitution satisfies the predicate
     */
    public boolean isSatisfied(Answer sub) {
        return !sub.containsVar(getVarName())
                || !sub.containsVar(getReferenceVarName())
                || !sub.get(getVarName()).equals(sub.get(getReferenceVarName()));
    }

    private Var getReferenceVarName(){
        return getPredicate();
    }
}
