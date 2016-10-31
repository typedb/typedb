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

package io.mindmaps.graql.internal.reasoner.atom;

import com.google.common.collect.Sets;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.Patterns;
import io.mindmaps.graql.internal.reasoner.query.Query;

import java.util.*;

public abstract class AtomBase implements Atomic{

    protected String varName = null;
    protected PatternAdmin atomPattern = null;
    private Query parent = null;

    public AtomBase(VarAdmin pattern) {
        this.atomPattern = pattern;
        this.varName = pattern.getName();
    }

    public AtomBase(VarAdmin pattern, Query par) {
        this(pattern);
        this.parent = par;
    }

    public AtomBase(AtomBase a) {
        if (a.getParentQuery() != null)
            this.parent = a.getParentQuery();
        this.atomPattern = Patterns.mergeVars(Sets.newHashSet(a.atomPattern.asVar()));
        this.varName = atomPattern.asVar().getName();
    }

    public abstract Atomic clone();

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean containsVar(String name){ return getVarNames().contains(name);}

    @Override
    public String getVarName(){ return varName;}

    @Override
    public Set<String> getVarNames(){
        return Sets.newHashSet(varName);
    }

    public PatternAdmin getPattern(){ return atomPattern;}

    public Query getParentQuery(){
        return parent;
    }

    public void setParentQuery(Query q){ parent = q;}

    private void setVarName(String var){
        varName = var;
        atomPattern.asVar().setName(var);
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
}

