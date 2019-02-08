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

package grakn.core.graql.internal.reasoner.atom.predicate;

import com.google.auto.value.AutoValue;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.internal.reasoner.utils.IgnoreHashEquals;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 *
 */
@AutoValue
public abstract class NeqValuePredicate extends NeqPredicate {

    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract Variable getPredicate();
    @Nullable public abstract Object getValue();

    @Override @IgnoreHashEquals public abstract Statement getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();

    public static NeqValuePredicate create(Variable varName, @Nullable Variable var, @Nullable Object value, ReasonerQuery parent){
        Variable predicateVar = var != null? var : Graql.var().var().asUserDefined();
        Statement pattern = Graql.var(varName).val(Graql.neq(value != null? value : Graql.var(predicateVar)));
        return new AutoValue_NeqValuePredicate(varName, predicateVar, value, pattern, parent);
    }

    public static NeqValuePredicate create(Variable varName, grakn.core.graql.query.predicate.ValuePredicate pred, ReasonerQuery parent) {
        Statement innerVar = pred.getInnerVar().orElse(null);
        Variable var = innerVar != null? innerVar.var() : Graql.var().var().asUserDefined();
        Object value = pred.equalsValue().orElse(null);
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
        Object predicateVal = getPredicates(getPredicate(), ValuePredicate.class)
                .map(p -> p.getPredicate().equalsValue())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst().orElse(null);

        Object val = getValue() != null? getValue() : predicateVal;
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
