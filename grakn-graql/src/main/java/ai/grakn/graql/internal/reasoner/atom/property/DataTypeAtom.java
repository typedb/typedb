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

package ai.grakn.graql.internal.reasoner.atom.property;

import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;
import ai.grakn.graql.admin.Atomic;

/**
 *
 * <p>
 * Atomic corresponding to graql DataTypeProperty.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class DataTypeAtom extends AtomBase {

    private final ResourceType.DataType<?> datatype;

    public DataTypeAtom(VarName varName, DataTypeProperty prop, ReasonerQuery parent){
        super(Graql.var(varName).datatype(prop.getDataType()).admin(), parent);
        this.datatype = prop.getDataType();
    }

    private DataTypeAtom(DataTypeAtom a) {
        super(a);
        this.datatype = a.getDataType();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        DataTypeAtom a2 = (DataTypeAtom) obj;
        return this.getDataType().equals(a2.getDataType()) &&
                this.getDataType().equals(((DataTypeAtom)obj).getDataType());
    }

    @Override
    public int hashCode(){
        int hashCode = equivalenceHashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        DataTypeAtom a2 = (DataTypeAtom) obj;
        return this.getDataType().equals(a2.getDataType());
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.datatype.hashCode();
        return hashCode;
    }

    @Override
    public Atomic copy() { return new DataTypeAtom(this);}

    public ResourceType.DataType<?> getDataType(){ return datatype;}
}
