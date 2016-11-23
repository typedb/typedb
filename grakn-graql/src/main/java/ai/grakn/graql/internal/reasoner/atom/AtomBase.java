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

import com.google.common.collect.Sets;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.Query;

import java.util.*;

public abstract class AtomBase implements Atomic{

    protected String varName = null;
    protected PatternAdmin atomPattern = null;
    private Query parent = null;

    public AtomBase(VarAdmin pattern, Query par) {
        this.atomPattern = pattern;
        this.varName = pattern.getName();
        this.parent = par;
    }

    public AtomBase(AtomBase a) {
        this.atomPattern = Patterns.mergeVars(Sets.newHashSet(a.atomPattern.asVar()));
        this.varName = atomPattern.asVar().getVarName();
    }

    @Override
    public abstract Atomic clone();

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean containsVar(String name){ return getVarNames().contains(name);}

    @Override
    public boolean isUserDefinedName(){ return atomPattern.asVar().isUserDefinedName();}

    @Override
    public String getVarName(){ return varName;}

    @Override
    public Set<String> getVarNames(){
        return Sets.newHashSet(varName);
    }

    public Set<String> getSelectedNames(){
         Set<String> vars = getParentQuery().getSelectedNames();
        vars.retainAll(getVarNames());
        return vars;
    }
    public boolean isValueUserDefinedName(){ return false;}

    public PatternAdmin getPattern(){ return atomPattern;}

    public Query getParentQuery(){
        return parent;
    }

    public void setParentQuery(Query q){ parent = q;}

    private void setVarName(String var){
        varName = var;
        atomPattern.asVar().setVarName(var);
    }

    public void unify(String from, String to) {
        String var = getVarName();
        if (var.equals(from)) {
            setVarName(to);
        } else if (var.equals(to)) {
            setVarName("captured->" + var);
        }
    }

    public void unify(Map<String, String> unifiers){
        String var = getVarName();
        if (unifiers.containsKey(var)) {
            setVarName(unifiers.get(var));
        }
        else if (unifiers.containsValue(var)) {
            setVarName("captured->" + var);
        }
    }

    public abstract Map<String, String> getUnifiers(Atomic parentAtom);
    public Set<Predicate> getPredicates(){ return new HashSet<>();}
}

