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

import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import java.util.Map;

public class NotEquals extends AtomBase {

    private String refVarName;

    public NotEquals(String varName, NeqProperty prop, Query parent){
        super(Graql.var(varName).neq(Graql.var(prop.getProperty().replace("$",""))).admin(), parent);
        this.refVarName = prop.getProperty().replace("$","");
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
    public Atomic clone() { return new NotEquals(this);}

    private void setRefVarName(String var){
        refVarName = var;
        atomPattern = Graql.var(varName).neq(Graql.var(var)).admin();
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        String var = getReferenceVarName();
        if (var.equals(from)) {
            setRefVarName(to);
        } else if (var.equals(to)) {
            setRefVarName("captured->" + var);
        }
    }

    @Override
    public void unify(Map<String, String> unifiers){
        super.unify(unifiers);
        String var = getReferenceVarName();
        if (unifiers.containsKey(var)) {
            setRefVarName(unifiers.get(var));
        }
        else if (unifiers.containsValue(var)) {
            setRefVarName("captured->" + var);
        }
    }

    public String getReferenceVarName(){ return refVarName;}

    public QueryAnswers filter(QueryAnswers answers){
        QueryAnswers results = new QueryAnswers();
        answers.stream()
                .filter(answer -> !answer.get(varName).equals(answer.get(refVarName)))
                .forEach(results::add);
        return results;
    }
}
