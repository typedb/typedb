/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an id predicate. Corresponds to {@link IdProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class IdPredicate extends Predicate<ConceptId>{

    /*
    private IdPredicate(VarPattern pattern, ReasonerQuery par) {
        super(pattern, par);
    }
    private IdPredicate(Var varName, Label label, ReasonerQuery parent) { super(createIdVar(varName.asUserDefined(), label, parent.tx()), parent);}
    private IdPredicate(Var varName, ConceptId id, ReasonerQuery parent) {
        super(createIdVar(varName.asUserDefined(), id), parent);
    }
    private IdPredicate(Var varName, Concept con, ReasonerQuery parent) {
        super(createIdVar(varName.asUserDefined(), con.getId()), parent);
    }
    private IdPredicate(IdPredicate a, ReasonerQuery parent) { super(a, parent);}
    */

    public static IdPredicate create(VarPattern pattern, ReasonerQuery parent) {
        IdPredicate predicate = new AutoValue_IdPredicate(pattern.admin().var(), pattern, extractPredicate(pattern));
        predicate.parent = parent;
        return predicate;
    }
    public static IdPredicate create(Var varName, Label label, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), label, parent.tx()), parent);
    }
    public static IdPredicate create(Var varName, ConceptId id, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), id), parent);
    }
    public static IdPredicate create(Var varName, Concept con, ReasonerQuery parent) {
        return create(createIdVar(varName.asUserDefined(), con.getId()), parent);
    }
    private static IdPredicate create(IdPredicate a, ReasonerQuery parent) {
        return create(a.getPattern(), parent);
    }

    private static ConceptId extractPredicate(VarPattern var){
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

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public void checkValid() {
        ConceptId conceptId = getPredicate();
        if (tx().getConcept(conceptId) == null){
            throw GraqlQueryException.idNotFound(conceptId);
        }
    }

    @Override
    public String toString(){
        return "[" + getVarName() + "/" + getPredicateValue() + "]";
    }

    @Override
    public String getPredicateValue() { return getPredicate().getValue();}
}
