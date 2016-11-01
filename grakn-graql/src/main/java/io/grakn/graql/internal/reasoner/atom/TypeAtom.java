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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;

public class TypeAtom extends Atom{

    public TypeAtom(VarAdmin pattern) {
        super(pattern);
    }
    public TypeAtom(VarAdmin pattern, Query par) {
        super(pattern, par);
    }
    public TypeAtom(TypeAtom a) {
        super(a);
    }

    @Override
    public Atomic clone(){
        return new TypeAtom(this);
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TypeAtom)) return false;
        TypeAtom a2 = (TypeAtom) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof TypeAtom)) return false;
        TypeAtom a2 = (TypeAtom) obj;
        return this.typeId.equals(a2.getTypeId());
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        return hashCode;
    }
}

