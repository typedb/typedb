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

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.pattern.property.IdProperty;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an id predicate. Corresponds to {@link IdProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class IdPredicate extends Predicate<ConceptId>{

    public IdPredicate(VarPattern pattern, ReasonerQuery par) {
        super(pattern, par);
    }
    public IdPredicate(Var varName, Label label, ReasonerQuery par) { super(createIdVar(varName.asUserDefined(), label, par.tx()), par);}
    public IdPredicate(Var varName, ConceptId id, ReasonerQuery par) {
        super(createIdVar(varName.asUserDefined(), id), par);
    }
    public IdPredicate(Var varName, Concept con, ReasonerQuery par) {
        super(createIdVar(varName.asUserDefined(), con.getId()), par);
    }
    private IdPredicate(IdPredicate a) { super(a);}

    @Override
    public String toString(){
        return "[" + getVarName() + "/" + getPredicateValue() + "]";
    }

    @Override
    public Atomic copy(){
        return new IdPredicate(this);
    }

    @Override
    public String getPredicateValue() { return getPredicate().getValue();}

    @Override
    protected ConceptId extractPredicate(VarPattern var){
        return var.admin().getProperty(IdProperty.class).map(IdProperty::id).orElse(null);
    }

    private static VarPattern createIdVar(Var varName, ConceptId typeId){
        return varName.id(typeId);
    }

    private static VarPattern createIdVar(Var varName, Label label, GraknTx graph){
        SchemaConcept schemaConcept = graph.getSchemaConcept(label);
        if (schemaConcept == null) throw GraqlQueryException.labelNotFound(label);
        return varName.id(schemaConcept.getId());
    }
}
