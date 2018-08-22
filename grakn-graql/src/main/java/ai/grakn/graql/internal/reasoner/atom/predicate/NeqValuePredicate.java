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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;
import com.google.common.base.Equivalence;

/**
 *
 */
@AutoValue
public abstract class NeqValuePredicate extends NeqPredicate {

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();
    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract Var getPredicate();

    public static NeqValuePredicate create(VarPattern pattern, ReasonerQuery parent) {
        return new AutoValue_NeqValuePredicate(pattern.admin().var(), pattern, parent, extractPredicateVar(pattern));
    }
    public static NeqValuePredicate create(Var varName, ai.grakn.graql.internal.query.predicate.NeqPredicate pred, ReasonerQuery parent) {
        VarPatternAdmin pattern = parent.tx().graql().parser()
                .parsePattern("{" + varName + " !== " + pred.getInnerVar().orElse(null) + ";}").admin()
                .varPatterns().iterator().next();
        return create(pattern, parent);
    }
    public static NeqValuePredicate create(NeqPredicate a, ReasonerQuery parent) {
        return create(a.getPattern(), parent);
    }

    private static Var extractPredicateVar(VarPattern pattern) {
        return pattern.admin().getProperties(ValueProperty.class).iterator().next().predicate().getInnerVar().get().var();
    }

    @Override
    boolean predicateBindingsEquivalent(NeqPredicate that, Equivalence<Atomic> equiv) {
        System.out.println("method not implemented!");
        return false;
    }

    @Override
    int bindingHash(Equivalence<Atomic> equiv) {
        System.out.println("method not implemented!");
        return 0;
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public String toString(){
        return "[" + getVarName() + " !== " + getPredicate() + "]";
    }

    @Override
    public boolean isSatisfied(ConceptMap sub) {
        System.out.println("method not implemented!");
        return false;
    }

}
