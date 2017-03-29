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

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;

import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.reasoner.Utility.capture;

/**
 *
 * <p>
 * Implementation of atom corresponding to graql NotEquals property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class NotEquals extends AtomBase {

    private VarName refVarName;

    public NotEquals(VarName varName, NeqProperty prop, ReasonerQuery parent){
        super(var(varName).neq(var(prop.getVar().getVarName())).admin(), parent);
        this.refVarName = prop.getVar().getVarName();
    }
    public NotEquals(NotEquals a){
        super(a);
        this.refVarName = a.getReferenceVarName();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        NotEquals a2 = (NotEquals) obj;
        return getVarName().equals(a2.getVarName()) &&
                getReferenceVarName().equals(a2.getReferenceVarName());
    }

    @Override
    public int hashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.varName.hashCode();
        hashCode = hashCode * 37 + this.refVarName.hashCode();
        return hashCode;
    }
    @Override
    public boolean isEquivalent(Object obj) { return true;}

    @Override
    public int equivalenceHashCode() { return 1;}

    @Override
    public Atomic copy() { return new NotEquals(this);}

    private void setRefVarName(VarName var){
        refVarName = var;
        atomPattern = var(varName).neq(var(var)).admin();
    }

    @Override
    public void unify(Unifier unifier){
        super.unify(unifier);
        VarName var = getReferenceVarName();
        if (unifier.containsKey(var)) {
            setRefVarName(unifier.get(var));
        } else if (unifier.containsValue(var)) {
            setRefVarName(capture(var));
        }
    }

    public VarName getReferenceVarName(){ return refVarName;}

    public static boolean notEqualsOperator(Answer answer, NotEquals atom) {
        return !answer.get(atom.varName).equals(answer.get(atom.refVarName));
    }


    /**
     * apply the not equals filter to answer set
     * @param answers the filter should be applied to
     * @return filtered answer set
     */
    public QueryAnswers filter(QueryAnswers answers){
        QueryAnswers results = new QueryAnswers();
        answers.stream()
                .filter(answer -> !answer.get(varName).equals(answer.get(refVarName)))
                .forEach(results::add);
        return results;
    }

    /**
     * apply the not equals filter to answer stream
     * @param answers the filter should be applied to
     * @return filtered answer stream
     */
    public Stream<Answer> filter(Stream<Answer> answers){
        return answers.filter(answer -> !answer.get(varName).equals(answer.get(refVarName)));
    }
}
