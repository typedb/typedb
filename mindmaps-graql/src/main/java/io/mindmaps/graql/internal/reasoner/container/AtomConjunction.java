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

package io.mindmaps.graql.internal.reasoner.container;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.internal.query.Patterns;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.mindmaps.graql.internal.reasoner.Utility.checkAtomsCompatible;

public class AtomConjunction {

    private final Set<Atomic> atomSet;

    public AtomConjunction(Conjunction<VarAdmin> conj){
        this.atomSet = getAtomSet(conj);
    }
    private AtomConjunction(Set<Atomic> aSet){
        this.atomSet = aSet;
    }

    private Set<Atomic> getAtomSet(PatternAdmin pattern) {
        Set<Atomic> atoms = new HashSet<>();

        Set<VarAdmin> vars = pattern.getVars();
        vars.forEach(var -> {
            Atomic atom = AtomicFactory.create(var);
            atoms.add(atom);
        });

        return atoms;
    }
    private Set<Atomic> getAtoms(){ return atomSet;}

    public boolean isEmpty(){ return atomSet.isEmpty();}
    public boolean contains(Atomic atom){ return atomSet.contains(atom);}

    public AtomConjunction add(Atomic atom){
        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        atoms.add(atom);
        return new AtomConjunction(atoms);
    }

    public AtomConjunction remove(Atomic atom){
        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        atoms.remove(atom);
        return new AtomConjunction(atoms);
    }

    public AtomConjunction conjunction(AtomConjunction conj, MindmapsGraph graph) {
        boolean isCompatible = true;
        for (Atomic cAtom : conj.getAtoms() ) {
            Iterator<Atomic> it = atomSet.iterator();
            while(it.hasNext() && isCompatible)
                isCompatible = checkAtomsCompatible(it.next(), cAtom, graph);
        }
        if (!isCompatible) return null;

        Set<Atomic> atoms = new HashSet<>();
        atomSet.forEach(a -> atoms.add(AtomicFactory.create(a)));
        conj.getAtoms().forEach(atom -> atoms.add(AtomicFactory.create(atom)) );
        return new AtomConjunction(atoms);
    }

    public Conjunction<VarAdmin> getConjunction(){
        Set<VarAdmin> vars = new HashSet<>();
        atomSet.forEach(a -> vars.add(a.getPattern().asVar()));
        return Patterns.conjunction(vars);
    }
}
