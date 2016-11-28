/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.query.Query;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Iterator;

public class ValuePredicate extends Predicate<ValuePredicateAdmin> {

    public ValuePredicate(VarAdmin pattern) {
        super(pattern);
    }
    public ValuePredicate(VarAdmin pattern, Query par) {
        super(pattern, par);
    }
    public ValuePredicate(String varName, ValueProperty prop, Query par){
        this(createValueVar(varName, prop.getPredicate()), par);}
    private ValuePredicate(ValuePredicate pred) { super(pred);}

    public static VarAdmin createValueVar(String name, ValuePredicateAdmin pred) {
        return Graql.var(name).value(pred).admin();
    }

    @Override
    public Atomic clone() {
        return new ValuePredicate(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Predicate a2 = (Predicate) obj;
        return this.getVarName().equals(a2.getVarName())
                && this.predicate.getClass().equals(a2.predicate.getClass())
                && this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.varName.hashCode();
        hashCode = hashCode * 37 + this.predicate.getClass().hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate a2 = (ValuePredicate) obj;
        return this.predicate.getClass().equals(a2.predicate.getClass()) &&
                this.getPredicateValue().equals(a2.getPredicateValue());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = super.equivalenceHashCode();
        hashCode = hashCode * 37 + this.predicate.getClass().getName().hashCode();
        return hashCode;
    }

    @Override
    public boolean isValuePredicate(){ return true;}

    @Override
    public String getPredicateValue() {
        return predicate.getPredicate().map(P::getValue).map(Object::toString).orElse("");
    }

    @Override
    protected ValuePredicateAdmin extractPredicate(VarAdmin pattern) {
        Iterator<ValueProperty> properties = pattern.getProperties(ValueProperty.class).iterator();
        ValueProperty property = properties.next();
        if (properties.hasNext())
            throw new IllegalStateException("Attempting creation of ValuePredicate atom with more than single predicate");
        return property.getPredicate();
    }
}
