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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.admin.Atomic;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an id predicate.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class IdPredicate extends Predicate<ConceptId>{

    public IdPredicate(VarAdmin pattern, ReasonerQuery par) {
        super(pattern, par);
    }
    public IdPredicate(VarName varName, IdProperty prop, ReasonerQuery par){
        this(createIdVar(varName, prop.getId()), par);
    }
    public IdPredicate(VarName varName, NameProperty prop, ReasonerQuery par){
        this(createIdVar(varName, par.graph().getType(prop.getNameValue()).getId()), par);
    }
    private IdPredicate(IdPredicate a) { super(a);}

    public IdPredicate(VarName varName, Concept con, ReasonerQuery par) {
        super(createIdVar(varName, con.getId()), par);
        this.predicate = con.getId();
    }

    public static VarAdmin createIdVar(VarName varName, ConceptId typeId){
        return Graql.var(varName).id(typeId).admin();
    }

    @Override
    public Atomic copy(){
        return new IdPredicate(this);
    }

    @Override
    public boolean isIdPredicate(){ return true;}

    @Override
    public String getPredicateValue() { return predicate.getValue();}

    @Override
    protected ConceptId extractPredicate(VarAdmin var){ return var.admin().getId().orElse(null);}
}
