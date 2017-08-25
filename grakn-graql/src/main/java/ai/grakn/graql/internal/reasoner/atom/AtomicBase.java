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

import ai.grakn.GraknTx;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 *
 * <p>
 * Base {@link Atomic} implementation providing basic functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomicBase implements Atomic {

    private final Var varName;
    private final PatternAdmin atomPattern;
    private ReasonerQuery parent = null;

    protected AtomicBase(VarPatternAdmin pattern, ReasonerQuery par) {
        this.atomPattern = pattern;
        this.varName = pattern.var();
        this.parent = par;
    }

    protected AtomicBase(AtomicBase a) {
        this.atomPattern = a.atomPattern;
        this.varName = atomPattern.asVarPattern().var();
        this.parent = a.getParentQuery();
    }

    @Override
    public abstract Atomic copy();

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean containsVar(Var name){ return getVarNames().contains(name);}

    @Override
    public boolean isUserDefinedName(){ return atomPattern.asVarPattern().var().isUserDefinedName();}
    
    @Override
    public Var getVarName(){ return varName;}

    @Override
    public Set<Var> getVarNames(){
        return Sets.newHashSet(varName);
    }

    @Override
    public PatternAdmin getPattern(){ return atomPattern;}

    @Override
    public PatternAdmin getCombinedPattern(){ return getPattern();}

    @Override
    public ReasonerQuery getParentQuery(){ return parent;}

    @Override
    public void setParentQuery(ReasonerQuery q){ parent = q;}

    @Override
    public Atomic inferTypes(){ return this; }

    /**
     * @return GraknTx this atomic is defined in
     */
    protected GraknTx tx(){ return getParentQuery().tx();}
}

