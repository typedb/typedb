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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.reasoner.query.Query;

public class IdPredicate extends Predicate<String>{

    private String typeName = "";

    public IdPredicate(VarAdmin pattern) {
        super(pattern);
    }
    public IdPredicate(VarAdmin pattern, Query par) {
        super(pattern, par);
    }
    public IdPredicate(String varName, IdProperty prop, Query par){
        this(createIdVar(varName, prop.getId()), par);
    }

    public IdPredicate(String varName, NameProperty prop, Query par){
        this(createIdVar(varName, par.graph().getType(prop.getNameValue()).getId()), par);
        typeName = prop.getNameValue();
    }
    public IdPredicate(IdPredicate a) {
        super(a);
    }

    public IdPredicate(String varName, Concept con) {
        super(createIdVar(varName, con.getId()));
        this.predicate = con.getId();
    }

    public static VarAdmin createIdVar(String varName, String typeId){
        return Graql.var(varName).id(typeId).admin();
    }

    public static VarAdmin createIdVarFromTypeName(String varName, String typeName, GraknGraph graph){
        return Graql.var(varName).id(graph.getType(typeName).getId()).admin();
    }

    @Override
    public Atomic clone(){
        return new IdPredicate(this);
    }

    @Override
    public boolean isIdPredicate(){ return true;}

    @Override
    public String getPredicateValue() { return predicate;}

    @Override
    protected String extractPredicate(VarAdmin var){ return var.admin().getId().orElse("");}
}
