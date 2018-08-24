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

import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.query.predicate.Predicates;
import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 *
 */
@AutoValue
public abstract class NeqValuePredicate extends NeqPredicate {

    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract Var getPredicate();
    @Nullable public abstract Object getValue();

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();

    public static NeqValuePredicate create(Var varName, Var var, @Nullable Object value, ReasonerQuery parent){
        VarPattern pattern = varName.val(Predicates.neq(value != null ? value : var));
        return new AutoValue_NeqValuePredicate(varName, var, value, pattern, parent);
    }

    public static NeqValuePredicate create(Var varName, ai.grakn.graql.internal.query.predicate.NeqPredicate pred, ReasonerQuery parent) {
        VarPatternAdmin innerVar = pred.getInnerVar().orElse(null);
        Var var = innerVar != null? innerVar.var() : Graql.var();
        Object value = pred.value().orElse(null);
        return create(varName, var, value, parent);
    }

    public static NeqValuePredicate create(NeqValuePredicate a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicate(), a.getValue(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public String toString(){
        return "[" + getVarName() + " !== " + getPredicate() + "]"
                + (getValue() != null? "[" + getPredicate() + "/" + getValue() + "]" : "");
    }

    @Override
    public boolean isSatisfied(ConceptMap sub) {
        ValuePredicate predicate = getPredicate(getPredicate(), ValuePredicate.class);
        Object val = getValue() != null?
                getValue() :
                predicate != null? predicate.getPredicate().equalsValue().orElse(null) : null;
        if (val == null &&
                (!sub.containsVar(getVarName()) || !sub.containsVar(getPredicate()))) {
            return true;
        }

        Concept concept = sub.containsVar(getVarName())? sub.get(getVarName()) : null;
        Concept referenceConcept = sub.containsVar(getPredicate())? sub.get(getPredicate()) : null;
        return  concept != null
                && concept.isAttribute()
                && (
                        (val != null && !concept.asAttribute().value().equals(val))
                                || (
                                        referenceConcept != null
                                                && referenceConcept.isAttribute()
                                                && !concept.asAttribute().value().equals(referenceConcept.asAttribute().value())
                        )
        );
    }

}
