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
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.Query;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.graql.internal.reasoner.Utility.CAPTURE_MARK;


/**
 *
 * <p>
 * Base atom implementation providing basic functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomBase implements Atomic{

    protected String varName = null;
    protected PatternAdmin atomPattern = null;
    private Query parent = null;

    protected AtomBase(VarAdmin pattern, Query par) {
        this.atomPattern = pattern;
        this.varName = pattern.getVarName();
        this.parent = par;
    }

    protected AtomBase(AtomBase a) {
        //TODO replace with proper clone method!
        this.atomPattern = Graql.parsePatterns(a.atomPattern.asVar().toString().concat(";")).iterator().next().admin();
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

    public void resetNames(){
        Map<String, String> unifiers = new HashMap<>();
        getVarNames().forEach(var -> unifiers.put(var, UUID.randomUUID().toString()));
        unify(unifiers);
    }

    /**
     * @return true if the value variable is user defined
     */
    public boolean isValueUserDefinedName(){ return false;}

    /**
     * @return pattern corresponding to this atom
     */
    public PatternAdmin getPattern(){ return atomPattern;}

    /**
     * @return the query the atom is contained in
     */
    public Query getParentQuery(){
        return parent;
    }

    /**
     * @param q query this atom is supposed to belong to
     */
    public void setParentQuery(Query q){ parent = q;}

    private void setVarName(String var){
        varName = var;
        atomPattern.asVar().setVarName(var);
    }

    /**
     * perform unification on the atom by applying unifier [from/to]
     * @param from variable name to be changed
     * @param to new variable name
     */
    public void unify(String from, String to) {
        String var = getVarName();
        if (var.equals(from)) setVarName(to);
        else if (var.equals(to)) setVarName(CAPTURE_MARK + var);
    }

    /**
     * perform unification on the atom by applying unifiers
     * @param unifiers contain variable mappings to be applied
     */
    public void unify(Map<String, String> unifiers){
        String var = getVarName();
        if (unifiers.containsKey(var)) setVarName(unifiers.get(var));
        else if (unifiers.containsValue(var)) setVarName(CAPTURE_MARK + var);
    }

    /**
     * get unifiers by comparing this atom with parent
     * @param parentAtom atom defining variable names
     * @return map of unifiers
     */
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (parentAtom.getClass() != this.getClass())
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        Map<String, String> map = new HashMap<>();
        if (!this.getVarName().equals(parentAtom.getVarName()))
            map.put(this.getVarName(), parentAtom.getVarName());
        return map;
    }
}

