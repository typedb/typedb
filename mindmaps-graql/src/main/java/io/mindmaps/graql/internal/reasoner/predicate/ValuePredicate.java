/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner.predicate;

import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.query.Query;
import java.util.Set;

public class ValuePredicate extends AtomBase {

    private final ValuePredicateAdmin predicate;

    public ValuePredicate(VarAdmin pattern) {
        super(pattern);
        Set<ValuePredicateAdmin> predicates = pattern.getValuePredicates();
        if (predicates.size() > 1)
            throw new IllegalStateException("Attempting creation of ValuePredicate atom with more than single predicate");
        this.predicate = predicates.iterator().next();
    }

    public ValuePredicate(VarAdmin pattern, Query par) {
        super(pattern, par);
        Set<ValuePredicateAdmin> predicates = pattern.getValuePredicates();
        if (predicates.size() > 1)
            throw new IllegalStateException("Attempting creation of ValuePredicate atom with more than single predicate");
        this.predicate = predicates.iterator().next();
    }

    public ValuePredicate(ValuePredicate pred) {
        this(pred.getVarName(), pred.predicate, pred.getParentQuery());
    }

    public ValuePredicate(String name, ValuePredicateAdmin pred, Query parent) {
        super(createValuePredicate(name, pred), parent);
        this.predicate = pred;
    }

    public static VarAdmin createValuePredicate(String name, ValuePredicateAdmin pred) {
        return Graql.var(name).value(pred).admin();
    }

    @Override
    public Atomic clone() {
        return new ValuePredicate(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ValuePredicate)) return false;
        ValuePredicate a2 = (ValuePredicate) obj;
        return this.getVarName().equals(a2.getVarName())
                && this.getVal().equals(a2.getVal());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getVal().hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj){
        if (!(obj instanceof ValuePredicate)) return false;
        ValuePredicate a2 = (ValuePredicate) obj;
        return this.getVal().equals(a2.getVal());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getVal().hashCode();
        return hashCode;
    }

    @Override
    public boolean isValuePredicate(){ return true;}

    @Override
    public String getVal(){
            return predicate.getPredicate().getValue().toString();
    }
}
